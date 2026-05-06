-- goncordia schema v1

CREATE TABLE IF NOT EXISTS goncordia_jobs (
    id           BIGSERIAL    PRIMARY KEY,
    queue        TEXT         NOT NULL,
    kind         TEXT         NOT NULL,
    args         JSONB        NOT NULL DEFAULT '{}',
    state        TEXT         NOT NULL DEFAULT 'available',
    priority     SMALLINT     NOT NULL DEFAULT 0,
    run_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    attempted_at TIMESTAMPTZ,
    finalized_at TIMESTAMPTZ,
    attempt_num  SMALLINT     NOT NULL DEFAULT 0,
    max_retry    SMALLINT     NOT NULL DEFAULT 0,
    timeout_ms   BIGINT       NOT NULL DEFAULT 0,
    unique_key   TEXT,
    worker_id    TEXT,
    tags         TEXT[]       NOT NULL DEFAULT '{}',
    errors       JSONB        NOT NULL DEFAULT '[]'
);

-- Fast fetch: available jobs ordered by priority desc, run_at asc
CREATE INDEX IF NOT EXISTS goncordia_jobs_fetch
    ON goncordia_jobs (queue, priority DESC, run_at)
    WHERE state = 'available';

-- Uniqueness within active states
CREATE UNIQUE INDEX IF NOT EXISTS goncordia_jobs_unique_key
    ON goncordia_jobs (queue, unique_key)
    WHERE unique_key IS NOT NULL
      AND state IN ('available', 'running', 'scheduled', 'retryable');

CREATE TABLE IF NOT EXISTS goncordia_queues (
    name       TEXT        PRIMARY KEY,
    paused     BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Trigger: NOTIFY after job insert so workers wake up immediately
CREATE OR REPLACE FUNCTION goncordia_notify_job_insert() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('goncordia:' || NEW.queue, NEW.id::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS goncordia_jobs_notify ON goncordia_jobs;
CREATE TRIGGER goncordia_jobs_notify
    AFTER INSERT ON goncordia_jobs
    FOR EACH ROW
    WHEN (NEW.state IN ('available', 'scheduled'))
    EXECUTE FUNCTION goncordia_notify_job_insert();
