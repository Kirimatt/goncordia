CREATE TABLE IF NOT EXISTS goncordia_jobs (
    id           BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    queue        VARCHAR(255) NOT NULL,
    kind         VARCHAR(255) NOT NULL,
    args         JSON         NOT NULL,
    state        VARCHAR(32)  NOT NULL DEFAULT 'available',
    priority     TINYINT      NOT NULL DEFAULT 0,
    run_at       DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    created_at   DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempted_at DATETIME(6),
    finalized_at DATETIME(6),
    attempt_num  SMALLINT     NOT NULL DEFAULT 0,
    max_retry    SMALLINT     NOT NULL DEFAULT 0,
    timeout_ms   BIGINT       NOT NULL DEFAULT 0,
    unique_key   VARCHAR(512),
    worker_id    VARCHAR(255),
    tags         JSON         NOT NULL DEFAULT (JSON_ARRAY()),
    errors       JSON         NOT NULL DEFAULT (JSON_ARRAY()),
    INDEX goncordia_jobs_fetch (queue, priority DESC, run_at),
    UNIQUE INDEX goncordia_jobs_unique_key (queue, unique_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS goncordia_queues (
    name       VARCHAR(255) NOT NULL PRIMARY KEY,
    paused     TINYINT(1)   NOT NULL DEFAULT 0,
    created_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
