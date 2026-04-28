package pgxqueues

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrClosed is returned by operations on a closed Client.
var ErrClosed = errors.New("pgxqueues: client is closed")

// Client is one named queue plus its worker pool. It holds one pool
// connection for LISTEN and up to WithWorkers concurrent handlers. Run
// blocks until ctx is cancelled. Enqueue is safe from any goroutine.
type Client[Args any] struct {
	pool    *pgxpool.Pool
	queue   string
	channel string
	handler Handler[Args]
	cfg     config

	ctx    context.Context
	cancel context.CancelFunc

	// wake is buffered to size 1 — multiple wakeups collapse into one.
	wake chan struct{}

	healthCh chan error

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// NewClient constructs a Client for queue with the given handler. The
// database must already have InstallSQL applied via migrations, unless
// WithRuntimeInstall(true) is passed. Call Run to start workers.
func NewClient[Args any](
	parent context.Context,
	pool *pgxpool.Pool,
	queue string,
	handler Handler[Args],
	opts ...Option,
) (*Client[Args], error) {
	if queue == "" {
		return nil, errors.New("pgxqueues: queue name is required")
	}
	if handler == nil {
		return nil, errors.New("pgxqueues: handler is required")
	}

	cfg := defaults()
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.runtimeInstall {
		if err := runtimeInstallBase(parent, pool); err != nil {
			return nil, fmt.Errorf("pgxqueues: runtime install: %w", err)
		}
	} else {
		if err := validateInstallBase(parent, pool); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(parent)
	c := &Client[Args]{
		pool:     pool,
		queue:    queue,
		channel:  notifyChannelPrefix + queue,
		handler:  handler,
		cfg:      cfg,
		ctx:      ctx,
		cancel:   cancel,
		wake:     make(chan struct{}, 1),
		healthCh: make(chan error, 1),
	}
	return c, nil
}

// Enqueue inserts a job inside the caller's transaction. The job
// becomes visible to workers only when the caller commits; rollback
// produces no job and no NOTIFY (transactional outbox).
func (c *Client[Args]) Enqueue(ctx context.Context, tx pgx.Tx, args Args) error {
	return c.enqueueAt(ctx, tx, args, time.Time{})
}

// EnqueueAt schedules a job to become eligible at or after t. Zero t
// behaves like Enqueue (immediate).
func (c *Client[Args]) EnqueueAt(ctx context.Context, tx pgx.Tx, args Args, t time.Time) error {
	return c.enqueueAt(ctx, tx, args, t)
}

func (c *Client[Args]) enqueueAt(ctx context.Context, tx pgx.Tx, args Args, at time.Time) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.mu.Unlock()

	payload, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("pgxqueues: marshal args: %w", err)
	}

	if at.IsZero() {
		_, err = tx.Exec(ctx,
			`INSERT INTO pgxqueues_jobs (queue, args, max_attempts) VALUES ($1, $2::jsonb, $3)`,
			c.queue, payload, c.cfg.maxAttempts,
		)
	} else {
		_, err = tx.Exec(ctx,
			`INSERT INTO pgxqueues_jobs (queue, args, max_attempts, available_at) VALUES ($1, $2::jsonb, $3, $4)`,
			c.queue, payload, c.cfg.maxAttempts, at,
		)
	}
	if err != nil {
		return fmt.Errorf("pgxqueues: insert job: %w", err)
	}
	c.cfg.metrics.JobEnqueued(c.queue)
	return nil
}

// Run starts the listen loop and worker pool, blocking until ctx is
// cancelled or Close is called. Call once per Client.
func (c *Client[Args]) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.mu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.listenLoop()
	}()

	for i := 0; i < c.cfg.workers; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.workerLoop()
		}()
	}

	<-ctx.Done()
	return c.Close()
}

// Health emits nil when the LISTEN connection is established (and on
// each recovery) and a non-nil error when it drops. The channel is
// buffered to size 1; emissions are dropped if no consumer is reading.
func (c *Client[Args]) Health() <-chan error { return c.healthCh }

// Close stops the worker pool, drains in-flight goroutines, and
// releases resources. Idempotent.
func (c *Client[Args]) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait()
	close(c.healthCh)
	return nil
}

func (c *Client[Args]) emitHealth(err error) {
	select {
	case c.healthCh <- err:
	default:
	}
}

func (c *Client[Args]) signalWake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
