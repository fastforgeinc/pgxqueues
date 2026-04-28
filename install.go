package pgxqueues

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func runtimeInstallBase(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, InstallSQL()); err != nil {
		return fmt.Errorf("execute install SQL: %w", err)
	}
	return nil
}

func validateInstallBase(ctx context.Context, pool *pgxpool.Pool) error {
	const q = `
		SELECT obj_description(p.oid, 'pg_proc')
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = current_schema()
		  AND p.proname = 'pgxqueues_notify_job'
	`
	var comment *string
	err := pool.QueryRow(ctx, q).Scan(&comment)
	if err != nil {
		return fmt.Errorf(
			"pgxqueues: required DDL not found. Install via migration:\n\n%s\n\nor pass WithRuntimeInstall(true) for dev/test. underlying error: %w",
			InstallSQL(), err,
		)
	}
	expected := "pgxqueues v" + Version
	if comment == nil || *comment != expected {
		got := "<nil>"
		if comment != nil {
			got = *comment
		}
		return fmt.Errorf(
			"pgxqueues: installed function version mismatch (have %q, want %q). Re-run install:\n\n%s",
			got, expected, InstallSQL(),
		)
	}
	return nil
}
