// Package pgxqueues provides a PostgreSQL-backed work queue on top of
// jackc/pgx/v5, with at-least-once delivery, NOTIFY-driven wakeup, and
// SKIP LOCKED-based competing consumers.
//
// # Overview
//
// pgxqueues stores jobs in a `pgxqueues_jobs` table and uses
// `SELECT ... FOR UPDATE SKIP LOCKED` so multiple worker processes can
// claim disjoint rows concurrently without coordination overhead.
// `LISTEN/NOTIFY` is used purely as a wakeup signal — the row in the
// jobs table is the source of truth.
//
// # Atomicity
//
// Enqueue takes a caller-supplied pgx.Tx so the job INSERT participates
// in the calling transaction. The job becomes visible to workers only
// when the caller commits; rolled-back transactions never produce a job.
// This is the transactional outbox pattern, applied directly to the
// queue table.
//
// # Delivery semantics
//
// At-least-once. A worker that crashes between claiming a job and
// completing the handler leaves the row in `claimed_at NOT NULL` state;
// it is automatically reclaimed after WithReclaimAfter (default 5
// minutes). Jobs that exceed `max_attempts` are left in the table with
// the last error in the `error` column — the dead-letter pattern is
// "row stays, query for it".
//
// # Usage
//
// Install the required DDL via golang-migrate migrations:
//
//	// In a migration:
//	//   pgxqueues.InstallSQL()  — table, index, function, trigger
//
// Or pass WithRuntimeInstall(true) for dev/test setups where migrations
// are not in use.
//
// Define a job-arguments struct, a handler, and run a Client:
//
//	type SendWelcomeEmail struct {
//	    UserID string `json:"user_id"`
//	    Email  string `json:"email"`
//	}
//
//	client, err := pgxqueues.NewClient[SendWelcomeEmail](
//	    ctx, pool, "welcome_email",
//	    func(ctx context.Context, job pgxqueues.Job[SendWelcomeEmail]) error {
//	        return mailer.SendWelcome(ctx, job.Args.UserID, job.Args.Email)
//	    },
//	    pgxqueues.WithWorkers(4),
//	)
//	if err != nil {
//	    return err
//	}
//	go client.Run(ctx)
//
//	// Enqueue inside the same TX as the row that owns the work:
//	tx, _ := pool.Begin(ctx)
//	users.Insert(ctx, tx, user)
//	client.Enqueue(ctx, tx, SendWelcomeEmail{UserID: user.ID, Email: user.Email})
//	tx.Commit(ctx)
package pgxqueues
