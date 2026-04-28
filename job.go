package pgxqueues

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Job is one unit of work passed to a Handler. Args is the JSON-decoded
// payload supplied at Enqueue time. Attempts is zero on first delivery.
type Job[Args any] struct {
	ID         uuid.UUID
	Queue      string
	Args       Args
	Attempts   int
	EnqueuedAt time.Time
}

// Handler processes a Job. Returning nil deletes the row; returning an
// error reschedules with backoff up to WithMaxAttempts, after which the
// row remains in the table with last_error set (dead-letter).
//
// The handler runs outside any transaction. Handlers must be idempotent
// — at-least-once delivery means a job may be re-run after a worker
// crashes between claim and completion.
type Handler[Args any] func(ctx context.Context, job Job[Args]) error
