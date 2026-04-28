package pgxqueues

// Logger is the structured-logger interface used for reconnects, claim
// errors, and handler failures. Adapt zap/slog/logrus to it. Default:
// NoopLogger.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// NoopLogger discards all log entries.
type NoopLogger struct{}

func (NoopLogger) Debug(string, ...any) {}
func (NoopLogger) Info(string, ...any)  {}
func (NoopLogger) Warn(string, ...any)  {}
func (NoopLogger) Error(string, ...any) {}
