package pgxqueues

import "time"

// Metrics is the metrics-sink interface used to surface queue health.
// Adapt Prometheus/OTEL/etc. to it. Default: NoopMetrics.
type Metrics interface {
	JobEnqueued(queue string)
	JobClaimed(queue string)
	JobCompleted(queue string, duration time.Duration)
	JobFailed(queue string, attempts int)
	ListenerUp(queue string)
	ListenerDown(queue string)
}

// NoopMetrics discards all metric calls.
type NoopMetrics struct{}

func (NoopMetrics) JobEnqueued(string)                 {}
func (NoopMetrics) JobClaimed(string)                  {}
func (NoopMetrics) JobCompleted(string, time.Duration) {}
func (NoopMetrics) JobFailed(string, int)              {}
func (NoopMetrics) ListenerUp(string)                  {}
func (NoopMetrics) ListenerDown(string)                {}
