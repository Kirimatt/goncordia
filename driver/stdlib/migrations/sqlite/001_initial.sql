CREATE TABLE IF NOT EXISTS goncordia_jobs (
    id           INTEGER  PRIMARY KEY AUTOINCREMENT,
    queue        TEXT     NOT NULL,
    kind         TEXT     NOT NULL,
    args         TEXT     NOT NULL DEFAULT '{}',
    state        TEXT     NOT NULL DEFAULT 'available',
    priority     INTEGER  NOT NULL DEFAULT 0,
    run_at       DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    created_at   DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    attempted_at DATETIME,
    finalized_at DATETIME,
    attempt_num  INTEGER  NOT NULL DEFAULT 0,
    max_retry    INTEGER  NOT NULL DEFAULT 0,
    timeout_ms   INTEGER  NOT NULL DEFAULT 0,
    unique_key   TEXT,
    worker_id    TEXT,
    tags         TEXT     NOT NULL DEFAULT '[]',
    errors       TEXT     NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS goncordia_jobs_fetch
    ON goncordia_jobs (queue, priority DESC, run_at)
    WHERE state IN ('available', 'scheduled');

CREATE UNIQUE INDEX IF NOT EXISTS goncordia_jobs_unique_key
    ON goncordia_jobs (queue, unique_key)
    WHERE unique_key IS NOT NULL
      AND state IN ('available', 'running', 'scheduled', 'retryable');

CREATE TABLE IF NOT EXISTS goncordia_queues (
    name       TEXT     PRIMARY KEY,
    paused     INTEGER  NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
