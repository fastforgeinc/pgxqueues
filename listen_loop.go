package pgxqueues

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

func (c *Client[Args]) listenLoop() {
	attempt := 0
	for c.ctx.Err() == nil {
		err := c.listenOnce()
		if c.ctx.Err() != nil {
			return
		}
		c.cfg.logger.Warn("pgxqueues: listen connection lost", "queue", c.queue, "error", err)
		c.cfg.metrics.ListenerDown(c.queue)
		c.emitHealth(err)

		delay := backoffDelay(c.cfg.backoff, attempt)
		attempt++
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (c *Client[Args]) listenOnce() error {
	conn, err := c.pool.Acquire(c.ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(c.ctx, "LISTEN "+c.channel); err != nil {
		return err
	}

	c.cfg.logger.Info("pgxqueues: listen up", "queue", c.queue, "channel", c.channel)
	c.cfg.metrics.ListenerUp(c.queue)
	c.emitHealth(nil)

	// Nudge workers on (re)connect — there may be rows already waiting
	// that arrived before LISTEN was active.
	c.signalWake()

	for c.ctx.Err() == nil {
		_, err := conn.Conn().WaitForNotification(c.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		c.signalWake()
	}
	return nil
}

func backoffDelay(cfg BackoffConfig, attempt int) time.Duration {
	base := float64(cfg.InitialDelay)
	for i := 0; i < attempt; i++ {
		base *= cfg.Multiplier
		if base >= float64(cfg.MaxDelay) {
			base = float64(cfg.MaxDelay)
			break
		}
	}
	if cfg.Jitter > 0 {
		spread := base * cfg.Jitter
		base += spread * (rand.Float64()*2 - 1)
	}
	if base < 0 {
		base = 0
	}
	return time.Duration(base)
}
