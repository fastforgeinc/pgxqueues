package pgxqueues

import "time"

// Option configures a Client at construction. Later options for the
// same setting overwrite earlier ones.
type Option func(*config)

type config struct {
	workers        int
	maxAttempts    int
	backoff        BackoffConfig
	reclaimAfter   time.Duration
	pollInterval   time.Duration
	runtimeInstall bool
	logger         Logger
	metrics        Metrics
}

func defaults() config {
	return config{
		workers:        4,
		maxAttempts:    5,
		backoff:        BackoffConfig{InitialDelay: 1 * time.Second, MaxDelay: 5 * time.Minute, Multiplier: 2.0, Jitter: 0.2},
		reclaimAfter:   5 * time.Minute,
		pollInterval:   30 * time.Second,
		runtimeInstall: false,
		logger:         NoopLogger{},
		metrics:        NoopMetrics{},
	}
}

// BackoffConfig controls exponential backoff with jitter between failed
// handler attempts. Delay before attempt N (1-indexed) is
// InitialDelay * Multiplier^(N-1), clamped to MaxDelay, then randomised
// by Jitter (fractional, in [0, 1]).
type BackoffConfig struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       float64
}

// WithWorkers sets the concurrent-handler count. Default: 4. Each
// worker holds one pool connection while processing; size your pool to
// at least workers + 1.
func WithWorkers(n int) Option {
	return func(c *config) { c.workers = n }
}

// WithMaxAttempts sets the per-job retry cap. Default: 5. Jobs that
// exceed this remain in the table with last_error set; the partial
// index excludes them from future claims (dead-letter).
func WithMaxAttempts(n int) Option {
	return func(c *config) { c.maxAttempts = n }
}

// WithBackoff sets the retry-backoff policy. Default: 1s initial,
// 5min max, ×2, ±20% jitter.
func WithBackoff(cfg BackoffConfig) Option {
	return func(c *config) { c.backoff = cfg }
}

// WithReclaimAfter sets how long a claimed row may sit before another
// worker may take it over (assumed-crashed handler). Default: 5min.
// Set to ≈2-3× worst-case handler runtime.
func WithReclaimAfter(d time.Duration) Option {
	return func(c *config) { c.reclaimAfter = d }
}

// WithPollInterval sets the periodic claim-attempt frequency, used as
// a safety net for missed NOTIFY signals. Default: 30s.
func WithPollInterval(d time.Duration) Option {
	return func(c *config) { c.pollInterval = d }
}

// WithRuntimeInstall makes NewClient apply InstallSQL at startup
// instead of expecting the DDL from migrations. Default: false.
// Requires DDL privileges on the database user.
func WithRuntimeInstall(install bool) Option {
	return func(c *config) { c.runtimeInstall = install }
}

// WithLogger attaches a structured logger. Default: NoopLogger.
func WithLogger(l Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithMetrics attaches a metrics sink. Default: NoopMetrics.
func WithMetrics(m Metrics) Option {
	return func(c *config) { c.metrics = m }
}
