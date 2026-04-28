package pgxqueues

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (c *Client[Args]) workerLoop() {
	pollTick := time.NewTicker(c.cfg.pollInterval)
	defer pollTick.Stop()

	for c.ctx.Err() == nil {
		if c.processOne() {
			continue
		}
		select {
		case <-c.ctx.Done():
			return
		case <-c.wake:
		case <-pollTick.C:
		}
	}
}

func (c *Client[Args]) processOne() bool {
	job, found, err := c.claim()
	if err != nil {
		c.cfg.logger.Warn("pgxqueues: claim error", "queue", c.queue, "error", err)
		return false
	}
	if !found {
		return false
	}
	c.cfg.metrics.JobClaimed(c.queue)

	start := time.Now()
	handlerErr := c.runHandler(job)
	duration := time.Since(start)

	if handlerErr == nil {
		c.complete(job.ID)
		c.cfg.metrics.JobCompleted(c.queue, duration)
		return true
	}

	c.cfg.logger.Warn("pgxqueues: handler error",
		"queue", c.queue, "job_id", job.ID, "attempts", job.Attempts+1, "error", handlerErr)
	c.fail(job, handlerErr)
	c.cfg.metrics.JobFailed(c.queue, job.Attempts+1)
	return true
}

func (c *Client[Args]) runHandler(job Job[Args]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pgxqueues: handler panic: %v", r)
		}
	}()
	return c.handler(c.ctx, job)
}

func (c *Client[Args]) claim() (Job[Args], bool, error) {
	tx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return Job[Args]{}, false, err
	}
	defer func() { _ = tx.Rollback(c.ctx) }()

	const selectQ = `
		SELECT id, args, attempts, created_at
		FROM pgxqueues_jobs
		WHERE queue = $1
		  AND attempts < max_attempts
		  AND (claimed_at IS NULL OR claimed_at < now() - $2::interval)
		  AND available_at <= now()
		ORDER BY available_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`
	var (
		id        uuid.UUID
		argsJSON  []byte
		attempts  int
		createdAt time.Time
	)
	err = tx.QueryRow(c.ctx, selectQ, c.queue, c.cfg.reclaimAfter).
		Scan(&id, &argsJSON, &attempts, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job[Args]{}, false, nil
	}
	if err != nil {
		return Job[Args]{}, false, err
	}

	if _, err := tx.Exec(c.ctx,
		`UPDATE pgxqueues_jobs SET claimed_at = now(), claimed_by = $1 WHERE id = $2`,
		hostname(), id,
	); err != nil {
		return Job[Args]{}, false, err
	}
	if err := tx.Commit(c.ctx); err != nil {
		return Job[Args]{}, false, err
	}

	var args Args
	if err := json.Unmarshal(argsJSON, &args); err != nil {
		// Force this row into DLQ — we cannot run the handler with a
		// malformed payload, and a clean attempts-bump would loop.
		_, _ = c.pool.Exec(c.ctx,
			`UPDATE pgxqueues_jobs
			 SET attempts = max_attempts,
			     last_error = $1,
			     claimed_at = NULL
			 WHERE id = $2`,
			"pgxqueues: decode error: "+err.Error(), id,
		)
		c.cfg.logger.Error("pgxqueues: decode error",
			"queue", c.queue, "job_id", id, "error", err)
		return Job[Args]{}, false, nil
	}

	return Job[Args]{
		ID:         id,
		Queue:      c.queue,
		Args:       args,
		Attempts:   attempts,
		EnqueuedAt: createdAt,
	}, true, nil
}

// complete is best-effort: a failed DELETE leaves the row claimed, but
// the reclaim window will pick it up later. Handlers are required to
// be idempotent, so a duplicate run is acceptable.
func (c *Client[Args]) complete(id uuid.UUID) {
	if _, err := c.pool.Exec(c.ctx,
		`DELETE FROM pgxqueues_jobs WHERE id = $1`, id,
	); err != nil {
		c.cfg.logger.Warn("pgxqueues: delete error",
			"queue", c.queue, "job_id", id, "error", err)
	}
}

func (c *Client[Args]) fail(job Job[Args], handlerErr error) {
	delay := backoffDelay(c.cfg.backoff, job.Attempts)
	_, err := c.pool.Exec(c.ctx,
		`UPDATE pgxqueues_jobs
		 SET attempts = attempts + 1,
		     claimed_at = NULL,
		     claimed_by = NULL,
		     last_error = $1,
		     available_at = now() + $2::interval
		 WHERE id = $3`,
		handlerErr.Error(), delay, job.ID,
	)
	if err != nil {
		c.cfg.logger.Warn("pgxqueues: fail-update error",
			"queue", c.queue, "job_id", job.ID, "error", err)
	}
	// Wake the pool when the retry becomes due. NOTIFY only fires on
	// INSERT, so without this nudge the worker waits for pollInterval.
	if job.Attempts+1 < c.cfg.maxAttempts {
		time.AfterFunc(delay, c.signalWake)
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
