# pgxqueues

PostgreSQL-backed work queue for Go, built on [jackc/pgx](https://github.com/jackc/pgx). Atomic enqueue inside the caller's transaction, `SELECT … FOR UPDATE SKIP LOCKED` competing-consumer claim, `LISTEN/NOTIFY`-driven wakeup, retry with backoff, dead-letter via the table itself.

```go
type SendWelcomeEmail struct {
    UserID string `json:"user_id"`
    Email  string `json:"email"`
}

client, err := pgxqueues.NewClient[SendWelcomeEmail](
    ctx, pool, "welcome_email",
    func(ctx context.Context, job pgxqueues.Job[SendWelcomeEmail]) error {
        return mailer.SendWelcome(ctx, job.Args.UserID, job.Args.Email)
    },
    pgxqueues.WithWorkers(4),
)
go client.Run(ctx)

// Enqueue inside the same TX as the row that owns the work:
tx, _ := pool.Begin(ctx)
users.Insert(ctx, tx, user)
client.Enqueue(ctx, tx, SendWelcomeEmail{UserID: user.ID, Email: user.Email})
tx.Commit(ctx)
```

## Why pgxqueues

You have Postgres. You need a work queue with at-least-once delivery and atomic enqueue (the work appears iff the calling transaction commits — no orphan jobs, no missed jobs). You don't want to add Kafka, RabbitMQ, or Redis just for this.

pgxqueues uses two Postgres primitives that have existed since 9.5:

- **`SELECT … FOR UPDATE SKIP LOCKED`** so N worker pods compete for jobs without blocking each other or needing leader election.
- **`LISTEN/NOTIFY`** as a wakeup signal, so workers don't busy-poll.

The job row is the source of truth. There is no separate queue store, no separate broker, no separate consumer-group state.

## Atomicity

`Enqueue(ctx, tx, args)` takes a `pgx.Tx` from the caller. The job INSERT participates in that transaction:

- The caller commits → the job becomes visible to workers and `NOTIFY` fires.
- The caller rolls back → no job is created. `NOTIFY` never fires.

This is the transactional outbox pattern, applied directly to the queue table. No outbox-row-then-publish dance, because `pg_notify` is itself transactional.

## Delivery semantics

**At-least-once.** Three failure modes:

1. **Handler returns an error** — the row's `attempts` is bumped, `available_at` is set to `now() + backoff`. The same job will be retried.
2. **Worker crashes mid-handler** — `claimed_at` stays set. After `WithReclaimAfter` (default 5min), another worker re-claims the row. Handlers must be idempotent.
3. **`attempts` reaches `max_attempts`** — the row stays in the table with `last_error` populated. The partial index excludes it from future claims, making it a dead-letter. Inspect with `SELECT * FROM pgxqueues_jobs WHERE attempts = max_attempts`.

## Installation

```sh
go get github.com/fastforgeinc/pgxqueues@latest
```

## Quick start

### 1. Install the DDL via migrations (recommended)

The library exports the canonical SQL as `InstallSQL()`. In your `golang-migrate` (or equivalent) migrations:

```go
// migrations/0001_pgxqueues_install.up.sql — generated from pgxqueues.InstallSQL()
```

`InstallSQL()` is idempotent (uses `CREATE … IF NOT EXISTS` / `CREATE OR REPLACE`) and creates:

- `pgxqueues_jobs` table + partial index on `(queue, available_at)` for ready-to-run rows
- `pgxqueues_notify_job()` PL/pgSQL function with a version stamp in its `COMMENT`
- `pgxqueues_jobs_notify` AFTER INSERT trigger

One install covers all queues. Each `Client` picks a queue name that becomes the second segment of the LISTEN channel (`pgxqueues_<queue>`).

### 2. Run a Client in your service

```go
import "github.com/fastforgeinc/pgxqueues"

pool, _ := pgxpool.New(ctx, dsn)

client, err := pgxqueues.NewClient[MyArgs](
    ctx, pool, "my_queue",
    func(ctx context.Context, job pgxqueues.Job[MyArgs]) error {
        return doWork(ctx, job.Args)
    },
)
if err != nil {
    return err
}
go client.Run(ctx)
```

`NewClient` validates that the installed DDL matches the running library `Version` via `pg_catalog`. Mismatches fail fast with an actionable error.

### Dev / test shortcut

For setups without migrations, opt in to runtime install:

```go
client, err := pgxqueues.NewClient[MyArgs](ctx, pool, "my_queue", handler,
    pgxqueues.WithRuntimeInstall(true),
)
```

This runs `InstallSQL()` at startup. Requires DDL privileges on the database user.

A runnable example lives in [`examples/basic`](examples/basic/main.go).

## Configuration

All options have sensible defaults; see `options.go` for full documentation.

| Option | Default | Description |
|---|---|---|
| `WithWorkers(int)` | `4` | Concurrent worker goroutines per Client. Each holds at most one pool connection while processing. |
| `WithMaxAttempts(int)` | `5` | Per-job retry cap before DLQ. |
| `WithBackoff(BackoffConfig)` | `1s` initial, `5min` max, ×2, ±20% jitter | Retry-backoff policy. |
| `WithReclaimAfter(d)` | `5min` | A claimed row may be re-claimed by another worker after this much time, in case the original worker crashed mid-handler. Set to ≈2-3× worst-case handler runtime. |
| `WithPollInterval(d)` | `30s` | Periodic safety poll for missed NOTIFY signals. Most claims are NOTIFY-driven; the poll rarely fires usefully. |
| `WithRuntimeInstall(bool)` | `false` | Install DDL at startup instead of expecting it from migrations. |
| `WithLogger(Logger)` | `NoopLogger` | Adapter to your logger (zap, slog, etc.). |
| `WithMetrics(Metrics)` | `NoopMetrics` | Adapter to your metrics registry. |

## Health signal & reconnection

`Client.Health() <-chan error` emits `nil` when the LISTEN connection is established (and again on each recovery), and a non-nil error when the connection drops. Mirrors the [pgxevents](https://github.com/fastforgeinc/pgxevents) pattern. Consumers may use this to drain downstream stream pools when the queue listener is unhealthy.

```go
go func() {
    for err := range client.Health() {
        if err != nil {
            // Queue listener is down — let dependents know.
            myStreamPool.DrainAll()
        }
    }
}()
```

The library reconnects automatically with exponential backoff and jitter. While the LISTEN connection is down, workers fall back to the periodic poll.

## Tradeoffs

- **At-least-once.** Handlers must be idempotent. Crashes mid-handler result in re-runs after the reclaim window.
- **One LISTEN connection per Client.** Sized into your pool — provision at least `WithWorkers(N)` + 1 connections.
- **NOTIFY is a single-instance primitive.** On a Postgres replica, `NOTIFY` does not fire (triggers don't run during WAL replay). Workers must connect to the primary. For multi-region active-active, use a real broker — this library is for single-write-region deployments.
- **JSON args.** `Args` is encoded with `encoding/json` on enqueue and decoded on claim. Keep the struct in sync with the producer's struct shape.
- **DLQ rows accumulate.** Failed-permanently rows stay in the table with `last_error`. Add a periodic cleanup if you don't want them piling up:

```sql
DELETE FROM pgxqueues_jobs
WHERE attempts >= max_attempts
  AND created_at < now() - interval '30 days';
```

## Architectural relationship to pgxevents

`pgxqueues` is a sibling to [`pgxevents`](https://github.com/fastforgeinc/pgxevents). Same author, same coding style, same install/validate model, same `Health()` pattern. They solve different problems and don't depend on each other:

| | pgxevents | pgxqueues |
|---|---|---|
| Use case | Pub/sub on row mutations (every subscriber gets every event) | Work queue (one worker consumes each job) |
| Trigger fires on | INSERT / UPDATE / DELETE | INSERT only |
| Storage | `pgxevents_outbox` event log + cleanup | `pgxqueues_jobs` is the queue itself |
| Claim | None — broadcast | `SELECT … FOR UPDATE SKIP LOCKED` |
| Delivery | At-most-once | At-least-once |

You can use both in the same service (different tables, separate listeners, no interaction).

## License

MIT. See [LICENSE](LICENSE).
