//go:build integration

package pgxqueues

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("pgxqueues_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		tc.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

type echoArgs struct {
	N int `json:"n"`
}

func TestIntegrationEnqueueAndConsume(t *testing.T) {
	pool := setupPostgres(t)
	mustExec(t, pool, InstallSQL())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got := make(chan int, 1)
	client, err := NewClient[echoArgs](ctx, pool, "echo",
		func(_ context.Context, job Job[echoArgs]) error {
			got <- job.Args.N
			return nil
		},
		WithWorkers(2),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	go func() { _ = client.Run(ctx) }()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := client.Enqueue(ctx, tx, echoArgs{N: 42}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	select {
	case n := <-got:
		if n != 42 {
			t.Fatalf("got %d, want 42", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler not called within 10s")
	}

	time.Sleep(200 * time.Millisecond)
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pgxqueues_jobs WHERE queue = 'echo'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected row deleted, got %d rows", count)
	}
}

func TestIntegrationRollbackDoesNotEnqueue(t *testing.T) {
	pool := setupPostgres(t)
	mustExec(t, pool, InstallSQL())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	called := false
	client, err := NewClient[echoArgs](ctx, pool, "rollback",
		func(_ context.Context, _ Job[echoArgs]) error {
			called = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	go func() { _ = client.Run(ctx) }()

	tx, _ := pool.Begin(ctx)
	_ = client.Enqueue(ctx, tx, echoArgs{N: 1})
	_ = tx.Rollback(ctx)

	time.Sleep(2 * time.Second)
	if called {
		t.Fatal("handler invoked for rolled-back job")
	}
}

func TestIntegrationRetryAndDLQ(t *testing.T) {
	pool := setupPostgres(t)
	mustExec(t, pool, InstallSQL())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var attempts int32
	client, err := NewClient[echoArgs](ctx, pool, "fail",
		func(_ context.Context, _ Job[echoArgs]) error {
			atomic.AddInt32(&attempts, 1)
			return errors.New("boom")
		},
		WithWorkers(1),
		WithMaxAttempts(3),
		WithBackoff(BackoffConfig{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
			Multiplier:   2.0,
		}),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	go func() { _ = client.Run(ctx) }()

	tx, _ := pool.Begin(ctx)
	_ = client.Enqueue(ctx, tx, echoArgs{N: 1})
	_ = tx.Commit(ctx)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&attempts) >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}

	// Row stays put with attempts == max_attempts and last_error set
	// — that is the DLQ shape.
	var (
		rowAttempts int
		maxAttempts int
		lastError   *string
	)
	err = pool.QueryRow(ctx,
		`SELECT attempts, max_attempts, last_error FROM pgxqueues_jobs WHERE queue = 'fail'`).
		Scan(&rowAttempts, &maxAttempts, &lastError)
	if err != nil {
		t.Fatalf("query DLQ row: %v", err)
	}
	if rowAttempts != maxAttempts {
		t.Fatalf("row attempts %d != max %d", rowAttempts, maxAttempts)
	}
	if lastError == nil || *lastError != "boom" {
		t.Fatalf("last_error = %v, want \"boom\"", lastError)
	}

	time.Sleep(2 * time.Second)
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("attempts kept growing after DLQ: %d", got)
	}
}

func TestIntegrationSkipLockedNoDoubleProcess(t *testing.T) {
	pool := setupPostgres(t)
	mustExec(t, pool, InstallSQL())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const n = 50
	var processed sync.Map
	var dupes atomic.Int32
	done := make(chan struct{}, n)

	client, err := NewClient[echoArgs](ctx, pool, "race",
		func(_ context.Context, job Job[echoArgs]) error {
			if _, loaded := processed.LoadOrStore(job.Args.N, struct{}{}); loaded {
				dupes.Add(1)
			}
			done <- struct{}{}
			return nil
		},
		WithWorkers(8),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	go func() { _ = client.Run(ctx) }()

	tx, _ := pool.Begin(ctx)
	for i := 0; i < n; i++ {
		if err := client.Enqueue(ctx, tx, echoArgs{N: i}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	_ = tx.Commit(ctx)

	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatalf("only got %d/%d jobs processed", i, n)
		}
	}
	if d := dupes.Load(); d != 0 {
		t.Fatalf("%d duplicate runs detected", d)
	}
}

func TestIntegrationValidateMissingFunction(t *testing.T) {
	pool := setupPostgres(t)

	_, err := NewClient[echoArgs](context.Background(), pool, "missing",
		func(_ context.Context, _ Job[echoArgs]) error { return nil },
	)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	// Error must include the install SQL so users can copy-paste it.
	if !contains(err.Error(), "CREATE OR REPLACE FUNCTION pgxqueues_notify_job") {
		t.Fatalf("error missing install SQL hint: %v", err)
	}
}

func TestIntegrationRuntimeInstall(t *testing.T) {
	pool := setupPostgres(t)

	_, err := NewClient[echoArgs](context.Background(), pool, "boot",
		func(_ context.Context, _ Job[echoArgs]) error { return nil },
		WithRuntimeInstall(true),
	)
	if err != nil {
		t.Fatalf("runtime install: %v", err)
	}
	var comment *string
	err = pool.QueryRow(context.Background(),
		`SELECT obj_description(p.oid, 'pg_proc')
		 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		 WHERE n.nspname = current_schema() AND p.proname = 'pgxqueues_notify_job'`).
		Scan(&comment)
	if err != nil {
		t.Fatalf("verify install: %v", err)
	}
	if comment == nil || *comment != "pgxqueues v"+Version {
		t.Fatalf("comment = %v, want pgxqueues v%s", comment, Version)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || index(haystack, needle) >= 0)
}

func index(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

var _ = fmt.Sprintf
