# goncordia

[![CI](https://github.com/kirimatt/goncordia/actions/workflows/ci.yml/badge.svg)](https://github.com/kirimatt/goncordia/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/kirimatt/goncordia.svg)](https://pkg.go.dev/github.com/kirimatt/goncordia)
[![GitHub release](https://img.shields.io/github/v/tag/kirimatt/goncordia?label=version)](https://github.com/kirimatt/goncordia/releases)

[Changelog](CHANGELOG.md)

A job queue engine for Go that works with the database you already have.

One `Driver[TTx]` interface parameterized by your library's native transaction type covers Postgres, MySQL, SQLite, MongoDB, Redis, Cassandra, ClickHouse, and in-memory — without forcing you to adopt a new dependency.

```go
tx, _ := pool.Begin(ctx)
_, _ = queries.CreateOrder(ctx, tx, order)
_, _ = client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: order.ID}, nil)
tx.Commit(ctx)  // job and order appear atomically
```

---

## Features

- **Transactional inserts** — `EnqueueTx` shares your existing transaction; the job appears if and only if that transaction commits
- **Scheduled jobs** — `InsertOpts.RunAt` for future execution
- **Priority queues** — higher priority processed first within a queue
- **Unique jobs** — deduplicate by kind, args, queue, or time window
- **Retry with backoff** — exponential (default), fixed, or custom `RetryPolicy`
- **Queue pause/resume** — drain a queue without stopping workers
- **Push notifications** — LISTEN/NOTIFY (Postgres), Change Streams (MongoDB), Pub/Sub (Redis); polling fallback elsewhere
- **SKIP LOCKED** — lock-free concurrent fetching on Postgres and MySQL
- **Periodic / cron jobs** — `CronScheduler` with `Every(d)` or custom `ScheduleFunc`
- **MockClock** — deterministic time control for tests; no `time.Sleep`

---

## Backends

| Driver | Package | Tx type | Atomic insert | Notes |
|---|---|---|---|---|
| PostgreSQL (pgx v5) | `driver/pgxv5` | `pgx.Tx` | ✅ | LISTEN/NOTIFY, advisory locks, SKIP LOCKED |
| PostgreSQL / MySQL / SQLite | `driver/stdlib` | `*sql.Tx` | ✅ | pgx stdlib, go-sql-driver/mysql, modernc sqlite |
| gorm | `driver/gorm` | `*gorm.DB` | ✅ | thin adapter over stdlib |
| bun | `driver/bun` | `bun.Tx` | ✅ | thin adapter over stdlib |
| MongoDB 4.0+ | `driver/mongodb` | `mongo.SessionContext` | ✅ | replica set required |
| Redis | `driver/redis` | `NoTx` | ❌ | at-least-once; Pub/Sub notifications |
| Cassandra 3.11+ | `driver/cassandra` | `NoTx` | ❌ | LWT claiming; ScyllaDB / DSE compatible |
| ClickHouse 23+ | `driver/clickhouse` | `NoTx` | ❌ | ReplacingMergeTree; at-least-once |
| In-memory | `driver/memory` | `memory.NoTx` | ✅ | no persistence; for tests |

---

## Installation

```bash
go get github.com/kirimatt/goncordia
```

Pick a driver:

```bash
# PostgreSQL via pgx v5
go get github.com/kirimatt/goncordia/driver/pgxv5 github.com/jackc/pgx/v5

# PostgreSQL / MySQL / SQLite via database/sql
go get github.com/kirimatt/goncordia/driver/stdlib

# gorm adapter
go get github.com/kirimatt/goncordia/driver/gorm gorm.io/gorm

# bun adapter
go get github.com/kirimatt/goncordia/driver/bun github.com/uptrace/bun

# MongoDB (replica set required)
go get github.com/kirimatt/goncordia/driver/mongodb go.mongodb.org/mongo-driver/mongo

# Redis
go get github.com/kirimatt/goncordia/driver/redis github.com/redis/go-redis/v9

# Cassandra / ScyllaDB
go get github.com/kirimatt/goncordia/driver/cassandra github.com/gocql/gocql

# ClickHouse
go get github.com/kirimatt/goncordia/driver/clickhouse github.com/ClickHouse/clickhouse-go/v2
```

---

## Quick start

### PostgreSQL (pgx v5)

```go
import (
    "github.com/kirimatt/goncordia"
    "github.com/kirimatt/goncordia/core"
    pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5"
    "github.com/jackc/pgx/v5/pgxpool"
)

pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
d := pgxdriver.New(pool)
d.Migrate(ctx)

type SendEmailArgs struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}
func (SendEmailArgs) Kind() string { return "send_email" }

registry := core.NewRegistry()
core.RegisterWorker(registry, core.WorkerFunc[SendEmailArgs](
    func(ctx context.Context, job *core.Job[SendEmailArgs]) error {
        return sendEmail(job.Args.To, job.Args.Subject)
    },
), core.WorkerOpts{MaxRetry: 5})

client := pgxdriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
    Queues:      []string{"default"},
    Concurrency: 10,
})
wp.Start(ctx)  // blocks; call wp.Stop() to drain gracefully
```

### Transactional insert (PostgreSQL)

```go
tx, _ := pool.Begin(ctx)
_, _ = queries.CreateOrder(ctx, tx, orderParams)
_, _ = client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: id}, nil)
tx.Commit(ctx)  // job and order are atomic
```

### MongoDB

```go
import (
    mongodriver "github.com/kirimatt/goncordia/driver/mongodb"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

client, _ := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URI")))
d, err := mongodriver.New(ctx, client, "myapp")  // fails if not a replica set
d.Migrate(ctx)

mqClient := mongodriver.NewClient(d, goncordia.ClientConfig{})

// Transactional insert via mongo.SessionContext
mongoClient.UseSession(ctx, func(sc mongo.SessionContext) error {
    sc.StartTransaction()
    db.Collection("orders").InsertOne(sc, order)
    mqClient.EnqueueTx(sc, sc, SendConfirmationArgs{OrderID: order.ID}, nil)
    return sc.CommitTransaction(sc)
})
```

### gorm

```go
import (
    gormdriver "github.com/kirimatt/goncordia/driver/gorm"
    "gorm.io/gorm"
)

d, _ := gormdriver.New(db)  // db is *gorm.DB
d.Migrate(ctx)

client := gormdriver.NewClient(d, goncordia.ClientConfig{})

db.Transaction(func(tx *gorm.DB) error {
    tx.Create(&order)
    client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: order.ID}, nil)
    return nil  // commit — job appears atomically with the order
})
```

### bun

```go
import (
    bundriver "github.com/kirimatt/goncordia/driver/bun"
    "github.com/uptrace/bun"
)

d := bundriver.New(db)  // db is *bun.DB
d.Migrate(ctx)

client := bundriver.NewClient(d, goncordia.ClientConfig{})

tx, _ := db.BeginTx(ctx, nil)
tx.NewInsert().Model(&order).Exec(ctx)
client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: order.ID}, nil)
tx.Commit()
```

### Redis

```go
import (
    redisdriver "github.com/kirimatt/goncordia/driver/redis"
    "github.com/redis/go-redis/v9"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
d := redisdriver.New(rdb)
d.Migrate(ctx)  // pings Redis to verify connectivity

client := redisdriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

// EnqueueTx is not supported on the Redis driver:
// there is no rollback guarantee. Use Enqueue (post-commit pattern) instead.
```

### Cassandra / ScyllaDB

```go
import (
    cassandradriver "github.com/kirimatt/goncordia/driver/cassandra"
    "github.com/gocql/gocql"
)

cluster := gocql.NewCluster("localhost")
cluster.Keyspace = "myapp"  // keyspace must already exist
session, _ := cluster.CreateSession()
defer session.Close()

d := cassandradriver.New(session)
d.Migrate(ctx)  // creates tables (idempotent)

client := cassandradriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

// EnqueueTx is identical to Enqueue on Cassandra — no rollback guarantee.
// Use idempotent workers and unique job options for deduplication.
```

### ClickHouse

```go
import (
    clickhousedriver "github.com/kirimatt/goncordia/driver/clickhouse"
    "github.com/ClickHouse/clickhouse-go/v2"
)

conn, _ := clickhouse.Open(&clickhouse.Options{
    Addr: []string{"localhost:9000"},
    Auth: clickhouse.Auth{Database: "myapp"},
})

d := clickhousedriver.New(conn)
d.Migrate(ctx)  // creates ReplacingMergeTree tables (idempotent)

client := clickhousedriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

// ClickHouse has no transactions. Jobs use at-least-once delivery — workers
// should be idempotent. Best suited for high-throughput analytics pipelines.
```

### SQLite (no Docker, good for tests)

```go
import (
    _ "modernc.org/sqlite"
    stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib"
)

db, _ := sql.Open("sqlite", "./jobs.db")
db.SetMaxOpenConns(1)  // SQLite: single writer

d := stdlibdriver.New(db, stdlibdriver.SQLite)
d.Migrate(ctx)
```

---

## Job lifecycle

```
available ──► running ──► completed
                │
                ├──► retryable ──► available  (scheduled retry)
                └──► discarded               (max retries exhausted)

available ──► cancelled   (via JobCancel)
scheduled ──► available   (when run_at is reached)
```

---

## InsertOpts

```go
maxRetry := 3
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, &core.InsertOpts{
    Queue:    "critical",                    // override default queue
    Priority: 10,                            // higher = processed first
    RunAt:    time.Now().Add(time.Hour),     // schedule for later

    UniqueOpts: &core.UniqueOpts{            // deduplicate
        ByArgs:  true,
        ByQueue: true,
    },

    MaxRetry: &maxRetry,
    Tags:     []string{"user:42"},
})
```

---

## WorkerConfig

```go
goncordia.WorkerConfig{
    Queues:          []string{"default", "critical"},
    Concurrency:     20,
    PollInterval:    500 * time.Millisecond,         // fallback when no push notifications
    RetryPolicy:     core.ExponentialRetry{Base: time.Second, Max: time.Hour},
    ShutdownTimeout: 30 * time.Second,
    Clock:           clock.NewMock(time.Now()),       // omit in production; inject for tests
}
```

---

## Periodic / cron jobs

`CronScheduler` enqueues jobs on a schedule. Pair it with a `WorkerPool` that processes them.

```go
import "github.com/kirimatt/goncordia/core"

cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
    {
        Schedule: core.Every(time.Hour),
        Args:     CleanupArgs{},
    },
    {
        Schedule: core.Every(24 * time.Hour),
        Args:     ReportArgs{},
        Opts:     &core.InsertOpts{Queue: "low-priority"},
    },
}, goncordia.CronConfig{
    TickInterval: time.Second, // how often to check for due jobs
})

go cs.Start(ctx)   // blocks; cancel ctx to stop
go wp.Start(ctx)   // worker pool processes the enqueued jobs
```

### Custom schedule

```go
// core.ScheduleFunc adapts any function to the Schedule interface.
sched := core.ScheduleFunc(func(last time.Time) time.Time {
    if last.IsZero() {
        return time.Time{} // run immediately on first tick
    }
    // Business-hours only: next run at 09:00 the following day
    next := last.Add(24 * time.Hour)
    next = time.Date(next.Year(), next.Month(), next.Day(), 9, 0, 0, 0, next.Location())
    return next
})
```

### Notes

- The scheduler fires each job on the **first tick** after `Start`, then respects the interval.
- `CronScheduler` only *enqueues* — workers run via `WorkerPool`.
- Add `UniqueOpts` to `PeriodicJob.Opts` to prevent duplicate jobs if multiple scheduler instances run.

---

## Retry policies

```go
// Exponential backoff (default): 1s, 2s, 4s, … capped at Max
core.ExponentialRetry{Base: time.Second, Max: 24 * time.Hour}

// Fixed delay
core.FixedRetry{Delay: 30 * time.Second}

// No retry — discard immediately
core.NoRetry{}

// Custom
import "github.com/kirimatt/goncordia/internal/clock"

type MyPolicy struct{}
func (MyPolicy) NextRetryAt(attempt int, err error, clk clock.Clock) time.Time {
    return clk.Now().Add(time.Duration(attempt) * time.Minute)
}
```

---

## Testing

Use the in-memory driver — no database, no Docker, deterministic time:

```go
import (
    "github.com/kirimatt/goncordia/driver/memory"
    "github.com/kirimatt/goncordia/internal/clock"
)

clk := clock.NewMock(time.Now())
d   := memory.New(memory.WithClock(clk))

client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{})
wp     := goncordia.NewWorkerPool[memory.NoTx](d, registry, goncordia.WorkerConfig{Clock: clk})

go wp.Start(ctx)
clk.Advance(time.Hour)  // trigger scheduled jobs instantly

jobs := d.AllJobs()  // inspect state without a real database
```

---

## Implementing a custom driver

Implement `driver.Driver[TTx]` where `TTx` is your transaction type:

```go
type MyDriver struct{}

func (d *MyDriver) Name() string                        { return "mydb" }
func (d *MyDriver) Capabilities() driver.Capabilities  { return driver.Capabilities{NativeTx: true} }
func (d *MyDriver) Executor() driver.Executor          { return &myExecutor{} }
func (d *MyDriver) UnwrapTx(tx MyTx) driver.ExecutorTx { return &myTxExecutor{tx: tx} }
func (d *MyDriver) Listener() driver.Listener          { return nil } // nil = polling fallback
func (d *MyDriver) Close() error                       { return nil }
```

`driver.Executor` has 14 methods covering job CRUD, queue management, and leader election. See [`driver/driver.go`](driver/driver.go) for the full interface and [`driver/memory/memory.go`](driver/memory/memory.go) for a minimal reference implementation.

---

## Benchmarks

```
go test ./bench/... -bench=. -benchmem -benchtime=5s -timeout=15m
```

Apple M5, single process. Memory/SQLite are in-process (no network); Postgres/MongoDB/Redis run in Docker on localhost.

**Enqueue — single job**

| Backend | ns/op | Notes |
|---|---|---|
| Memory | 0.57 µs | in-process mutex, no I/O |
| SQLite | 27 µs | WAL mode, single connection |
| Redis | 109 µs | ZADD over localhost |
| Postgres (pgx v5) | 129 µs | INSERT over localhost |
| MongoDB | 338 µs | insertOne over localhost |
| ClickHouse | 1 378 µs | INSERT + new data part over localhost |
| Cassandra | 7 216 µs | LWT requires Paxos quorum (3 round trips) |

**EnqueueBatch(100) — 100 jobs per call**

| Backend | ms/batch | jobs/s |
|---|---|---|
| Memory | 0.06 ms | ~1 775 000 |
| SQLite | 2.8 ms | ~35 300 |
| Redis | 10.9 ms | ~9 200 |
| Postgres (pgx v5) | 12.8 ms | ~7 800 |
| MongoDB | 34.9 ms | ~2 900 |
| ClickHouse | 150 ms | ~665 |
| Cassandra | 708 ms | ~141 |

**FetchAndComplete — hot worker loop path**

| Backend | µs/op | Notes |
|---|---|---|
| SQLite | 53 µs | indexed; faster than memory at scale |
| Memory | 520 µs | O(N) linear scan |
| Redis | 729 µs | Lua ZPOPMIN + HSET |
| MongoDB | 2 475 µs | findAndModify + updateOne |
| Postgres (pgx v5) | 12 190 µs | SELECT SKIP LOCKED + UPDATE |
| ClickHouse | 14 416 µs | SELECT FINAL + INSERT new version |
| Cassandra | 18 813 µs | SELECT avail + LWT UPDATE per job |

**End-to-end — 1 000 jobs, full WorkerPool**

| Backend | concurrency | jobs/s | Notes |
|---|---|---|---|
| Memory | c=10 | ~2 020 | |
| Redis | c=4 | ~1 084 | Pub/Sub notifications |
| SQLite | c=4 | ~800 | |
| MongoDB | c=4 | ~452 | Change Streams |
| Postgres (pgx v5) | c=4 | ~179 | LISTEN/NOTIFY |
| Cassandra | c=4 | ~153 | polling; LWT overhead |
| ClickHouse | c=4 | ~148 | polling; SELECT FINAL overhead |

End-to-end throughput is bounded by the 5 ms poll interval used in the benchmark. In production the pgxv5 driver uses LISTEN/NOTIFY and the Redis driver uses Pub/Sub, eliminating poll latency entirely — real throughput matches the FetchAndComplete numbers above.

Cassandra's high per-operation latency comes from Lightweight Transaction consensus (Paxos, ~3 network round trips per claim). ClickHouse's overhead comes from `SELECT … FINAL` deduplication at query time; both backends are best suited for workloads where high throughput matters more than low per-job latency.

---

## Observability (OpenTelemetry)

```bash
go get github.com/kirimatt/goncordia/otel
```

```go
import otelgoncordia "github.com/kirimatt/goncordia/otel"

wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
    Queues:      []string{"default"},
    Concurrency: 10,
    Middleware: []goncordia.JobMiddleware{
        otelgoncordia.NewMiddleware(
            // optional — defaults to otel.GetTracerProvider() / otel.GetMeterProvider()
            otelgoncordia.WithTracerProvider(tp),
            otelgoncordia.WithMeterProvider(mp),
        ),
    },
})
```

Each job execution produces:

- **Span** `goncordia.process` with attributes `goncordia.job.kind`, `goncordia.job.queue`, `goncordia.job.id`, `goncordia.job.attempt`
- **Histogram** `goncordia.job.duration` (seconds) — labelled by kind, queue, status
- **Counter** `goncordia.job.count` — labelled by kind, queue, status (`ok` / `error`)

Panics are recovered, converted to errors, and recorded on the span before re-triggering the retry policy — the worker pool always stays alive.

You can also add your own middleware for logging or custom metrics:

```go
func loggingMiddleware(ctx context.Context, job *core.RawJob, next func(context.Context, *core.RawJob) error) error {
    slog.InfoContext(ctx, "job started", "kind", job.Kind, "id", job.ID)
    err := next(ctx, job)
    slog.InfoContext(ctx, "job finished", "kind", job.Kind, "err", err)
    return err
}

goncordia.WorkerConfig{
    Middleware: []goncordia.JobMiddleware{
        otelgoncordia.NewMiddleware(),
        loggingMiddleware,
    },
}
```

---

## Project layout

```
goncordia/
├── client.go              # Client[TTx] — Enqueue, EnqueueTx, Cancel
├── worker.go              # WorkerPool[TTx] — Start, Stop, JobMiddleware
├── cron.go                # CronScheduler[TTx] — periodic/cron job scheduling
├── core/
│   ├── job.go             # JobArgs, Worker, InsertOpts, WorkerOpts
│   ├── registry.go        # type-erased worker dispatch
│   ├── retry.go           # RetryPolicy, ExponentialRetry, FixedRetry, NoRetry
│   └── schedule.go        # Schedule interface, Every, ScheduleFunc
├── driver/
│   ├── driver.go          # Driver[TTx], Executor, ExecutorTx, Listener interfaces
│   ├── memory/            # in-memory (no persistence; for tests)
│   ├── pgxv5/             # PostgreSQL via pgx v5 (LISTEN/NOTIFY, advisory locks)
│   ├── stdlib/            # PostgreSQL + MySQL + SQLite via database/sql
│   ├── gorm/              # gorm adapter (wraps stdlib)
│   ├── bun/               # bun adapter (wraps stdlib)
│   ├── mongodb/           # MongoDB 4.0+ replica set
│   ├── redis/             # Redis (at-least-once; Pub/Sub notifications)
│   ├── cassandra/         # Cassandra 3.11+ / ScyllaDB (LWT claiming; at-least-once)
│   └── clickhouse/        # ClickHouse 23+ (ReplacingMergeTree; at-least-once)
├── otel/                  # OpenTelemetry middleware (spans + metrics)
└── internal/clock/        # Clock interface + MockClock
```

---

## Transaction guarantees by backend

| Backend | Guarantee | Mechanism |
|---|---|---|
| Postgres / MySQL / SQLite | Atomic with business tx | Same DB connection, same `BEGIN`/`COMMIT` |
| gorm / bun | Atomic with business tx | Extracts underlying `*sql.Tx` |
| MongoDB | Atomic with business tx | Multi-document transaction on replica set |
| Redis | **None** — at-least-once | Pub/Sub + idempotent workers |
| Cassandra | **None** — at-least-once | LWT for claiming; no cross-statement tx |
| ClickHouse | **None** — at-least-once | ReplacingMergeTree + FINAL; no transactions |
| In-memory | Atomic (in-process) | Single mutex |

