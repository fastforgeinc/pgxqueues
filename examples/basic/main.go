// Runnable example demonstrating pgxqueues against a local Postgres.
//
// Prerequisites:
//
//	docker run --name pg -d -p 5432:5432 -e POSTGRES_PASSWORD=secret postgres:16
//	export DATABASE_URL=postgres://postgres:secret@localhost:5432/postgres?sslmode=disable
//	go run ./examples/basic
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fastforgeinc/pgxqueues"
)

type SendWelcomeEmail struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL not set")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Define the handler. Returning nil deletes the row; returning an
	// error reschedules with backoff up to WithMaxAttempts.
	handler := func(_ context.Context, job pgxqueues.Job[SendWelcomeEmail]) error {
		log.Printf("sending welcome user_id=%s email=%s attempts=%d",
			job.Args.UserID, job.Args.Email, job.Attempts)
		// Simulate transient failure on the first try.
		if job.Attempts == 0 && job.Args.UserID == "fail-once" {
			return errors.New("transient error")
		}
		return nil
	}

	client, err := pgxqueues.NewClient[SendWelcomeEmail](
		ctx, pool, "welcome_email", handler,
		pgxqueues.WithRuntimeInstall(true), // dev shortcut
		pgxqueues.WithWorkers(2),
		pgxqueues.WithMaxAttempts(3),
		pgxqueues.WithBackoff(pgxqueues.BackoffConfig{
			InitialDelay: 500 * time.Millisecond,
			MaxDelay:     5 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.2,
		}),
	)
	if err != nil {
		log.Fatalf("new client: %v", err)
	}

	go func() {
		if err := client.Run(ctx); err != nil {
			log.Printf("client run: %v", err)
		}
	}()

	for _, args := range []SendWelcomeEmail{
		{UserID: "user-1", Email: "alice@example.com"},
		{UserID: "fail-once", Email: "bob@example.com"},
		{UserID: "user-3", Email: "carol@example.com"},
	} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			log.Fatalf("begin: %v", err)
		}
		if err := client.Enqueue(ctx, tx, args); err != nil {
			_ = tx.Rollback(ctx)
			log.Fatalf("enqueue: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			log.Fatalf("commit: %v", err)
		}
	}

	<-ctx.Done()
	log.Println("shutdown")
}
