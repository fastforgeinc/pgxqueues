package pgxqueues

import (
	_ "embed"
)

// Version is stamped into the COMMENT on the installed
// pgxqueues_notify_job function. NewClient verifies it matches at
// startup so a stale DDL install fails fast.
const Version = "1.0.0"

const notifyChannelPrefix = "pgxqueues_"

//go:embed sql/install.sql
var installSQL string

// InstallSQL returns the idempotent DDL for the pgxqueues table, index,
// notify function, and trigger. Embed it in a migration, or pass
// WithRuntimeInstall(true) to apply it at startup. One install covers
// all queues used by the application.
func InstallSQL() string { return installSQL }
