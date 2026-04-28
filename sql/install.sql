-- pgxqueues v1.0.0 — work-queue on Postgres with SKIP LOCKED claim and
-- LISTEN/NOTIFY wakeup. Source: https://github.com/fastforgeinc/pgxqueues

CREATE TABLE IF NOT EXISTS pgxqueues_jobs (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    queue         text        NOT NULL,
    args          jsonb       NOT NULL,
    attempts      int         NOT NULL DEFAULT 0,
    max_attempts  int         NOT NULL DEFAULT 5,
    claimed_at    timestamptz,
    claimed_by    text,
    last_error    text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    available_at  timestamptz NOT NULL DEFAULT now()
);

-- Partial index serves the worker's claim query: rows ready to run, in
-- FIFO order. Excludes already-claimed and DLQ-stuck rows so the index
-- stays small on busy queues.
CREATE INDEX IF NOT EXISTS pgxqueues_jobs_available_idx
    ON pgxqueues_jobs (queue, available_at)
    WHERE claimed_at IS NULL AND attempts < max_attempts;

CREATE OR REPLACE FUNCTION pgxqueues_notify_job() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('pgxqueues_' || NEW.queue, NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION pgxqueues_notify_job() IS 'pgxqueues v1.0.0';

CREATE OR REPLACE TRIGGER pgxqueues_jobs_notify
    AFTER INSERT ON pgxqueues_jobs
    FOR EACH ROW EXECUTE FUNCTION pgxqueues_notify_job();
