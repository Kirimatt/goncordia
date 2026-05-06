# goncordia

A transactional job queue engine for Go. Works with the database you already have — PostgreSQL, MySQL, SQLite, or in-memory — through a single `Driver[TTx]` interface parameterized by your library's native transaction type.

**Key property:** when you call `EnqueueTx`, the job enters the queue if and only if the surrounding business transaction commits. No separate broker, no outbox table, no dual writes.

```go
tx, _ := pool.Begin(ctx)
_, _ = queries.CreateOrder(ctx, tx, order)
_, _ = client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: order.ID}, nil)
tx.Commit(ctx)  // job and order appear atomically
```

---

## Features

- **Transactional inserts** — `EnqueueTx` shares your `*pgx.Tx` / `*sql.Tx`; atomicity is guaranteed by the database
- **Scheduled jobs** — `InsertOpts.RunAt` for future execution
- **Priority queues** — higher priority processed first within a queue
- **Unique jobs** — deduplicate by kind, args, queue, or time window
- **Retry with backoff** — exponential (default), fixed, or custom `RetryPolicy`
- **Queue pause/resume** — drain a queue without stopping workers
- **LISTEN/NOTIFY** — push-based dispatch for pgxv5 (zero-latency); polling fallback for everything else
- **SKIP LOCKED** — lock-free concurrent fetching on Postgres and MySQL
- **MockClock** — deterministic time control for tests; no `time.Sleep`

---

## Backends

| Driver | Package | Tx type | Notes |
|---|---|---|---|
| PostgreSQL (pgx v5) | `driver/pgxv5` | `pgx.Tx` | LISTEN/NOTIFY, advisory locks, SKIP LOCKED |
| PostgreSQL (database/sql) | `driver/stdlib` | `*sql.Tx` | pgx stdlib adapter; SKIP LOCKED |
| MySQL 8.0+ | `driver/stdlib` | `*sql.Tx` | SKIP LOCKED |
| SQLite | `driver/stdlib` | `*sql.Tx` | single-writer, no SKIP LOCKED |
| In-memory | `driver/memory` | `memory.NoTx` | for tests; no persistence |

---

## Installation

```bash
go get github.com/goncordia/goncordia
```

Pick a driver:

```bash
# PostgreSQL via pgx
go get github.com/goncordia/goncordia/driver/pgxv5
go get github.com/jackc/pgx/v5

# PostgreSQL / MySQL / SQLite via database/sql
go get github.com/goncordia/goncordia/driver/stdlib

# PostgreSQL: go get github.com/jackc/pgx/v5/stdlib
# MySQL:      go get github.com/go-sql-driver/mysql
# SQLite:     go get modernc.org/sqlite
```

---

## Quick start

### PostgreSQL (pgx v5)

```go
import (
    "github.com/goncordia/goncordia"
    "github.com/goncordia/goncordia/core"
    pgxdriver "github.com/goncordia/goncordia/driver/pgxv5"
    "github.com/jackc/pgx/v5/pgxpool"
)

// 1. Connect and migrate
pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
d := pgxdriver.New(pool)
d.Migrate(ctx)

// 2. Define a job
type SendEmailArgs struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}
func (SendEmailArgs) Kind() string { return "send_email" }

// 3. Register a worker
registry := core.NewRegistry()
core.RegisterWorker(registry, core.WorkerFunc[SendEmailArgs](
    func(ctx context.Context, job *core.Job[SendEmailArgs]) error {
        return sendEmail(job.Args.To, job.Args.Subject)
    },
), core.WorkerOpts{MaxRetry: 5})

// 4. Enqueue a job
client := pgxdriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

// 5. Start a worker pool
pool := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
    Queues:      []string{"default"},
    Concurrency: 10,
})
pool.Start(ctx)  // blocks; call pool.Stop() to drain gracefully
```

### Transactional insert

```go
tx, _ := pool.Begin(ctx)
_, _ = db.CreateOrder(ctx, tx, orderParams)
_, _ = client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: id}, nil)
tx.Commit(ctx)  // both committed atomically, or both rolled back
```

### SQLite (no Docker, great for tests)

```go
import (
    _ "modernc.org/sqlite"
    "database/sql"
    "github.com/goncordia/goncordia/driver/stdlib"
)

db, _ := sql.Open("sqlite", "./jobs.db")
db.SetMaxOpenConns(1)  // SQLite: single writer

d := stdlib.New(db, stdlib.SQLite)
d.Migrate(ctx)

client := stdlib.NewClient(d, goncordia.ClientConfig{})
wp     := stdlib.NewWorkerPool(d, registry, goncordia.WorkerConfig{...})
```

### MySQL

```go
import (
    _ "github.com/go-sql-driver/mysql"
    "database/sql"
    "github.com/goncordia/goncordia/driver/stdlib"
)

db, _ := sql.Open("mysql", dsn+"?parseTime=true")
d := stdlib.New(db, stdlib.MySQL)
d.Migrate(ctx)
```

---

## Job lifecycle

```
available ──► running ──► completed
                │
                ├──► retryable ──► available (scheduled retry)
                │
                └──► discarded  (max retries exhausted)

available ──► cancelled  (via client.Cancel)
scheduled ──► available  (when run_at is reached)
```

---

## InsertOpts

```go
client.Enqueue(ctx, MyJobArgs{...}, &core.InsertOpts{
    Queue:    "critical",          // override default queue
    Priority: 10,                  // higher = earlier
    RunAt:    time.Now().Add(time.Hour),  // schedule for later

    // Deduplication: skip if identical job already active
    UniqueOpts: &core.UniqueOpts{
        ByArgs:  true,
        ByQueue: true,
    },

    MaxRetry: intPtr(3),           // override worker default
    Tags:     []string{"user:42"}, // for observability
})
```

---

## WorkerConfig

```go
goncordia.WorkerConfig{
    Queues:          []string{"default", "critical"},
    Concurrency:     20,
    PollInterval:    500 * time.Millisecond,  // only when no LISTEN/NOTIFY
    RetryPolicy:     core.ExponentialRetry{Base: time.Second, Max: time.Hour},
    ShutdownTimeout: 30 * time.Second,
    Clock:           clock.NewMock(time.Now()),  // for tests
}
```

---

## Retry policies

```go
// Exponential backoff: 1s, 2s, 4s, 8s… capped at Max (default)
core.ExponentialRetry{Base: time.Second, Max: 24 * time.Hour}

// Fixed delay
core.FixedRetry{Delay: 30 * time.Second}

// No retry — discard immediately on first failure
core.NoRetry{}

// Custom
type MyPolicy struct{}
func (MyPolicy) NextRetryAt(attempt int, err error, clk clock.Clock) time.Time {
    return clk.Now().Add(time.Duration(attempt) * time.Minute)
}
```

---

## Testing

Use the in-memory driver — no database, no Docker:

```go
import (
    "github.com/goncordia/goncordia/driver/memory"
    "github.com/goncordia/goncordia/internal/clock"
)

clk := clock.NewMock(time.Now())
d   := memory.New(memory.WithClock(clk))

client := goncordia.NewClient(d, goncordia.ClientConfig{})
wp     := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{Clock: clk})

// Advance mock time instead of sleeping
clk.Advance(time.Hour)

// Inspect internal state
jobs := d.AllJobs()
```

---

## Implementing a custom driver

```go
type MyDriver struct{}

func (d *MyDriver) Name() string                        { return "mydb" }
func (d *MyDriver) Capabilities() driver.Capabilities  { return driver.Capabilities{NativeTx: true} }
func (d *MyDriver) Executor() driver.Executor          { return &myExecutor{} }
func (d *MyDriver) UnwrapTx(tx MyTx) driver.ExecutorTx { return &myTxExecutor{tx: tx} }
func (d *MyDriver) Listener() driver.Listener          { return nil } // polling fallback
func (d *MyDriver) Close() error                       { return nil }
```

The `driver.Executor` interface requires ~14 methods covering job CRUD, queue management, and leader election. See `driver/driver.go` for the full interface.

---

## Project layout

```
goncordia/
├── client.go              # Client[TTx] — Enqueue, EnqueueTx, Cancel
├── worker.go              # WorkerPool[TTx] — Start, Stop
├── core/
│   ├── job.go             # JobArgs, Worker, InsertOpts, WorkerOpts
│   ├── registry.go        # type-erased worker dispatch
│   └── retry.go           # RetryPolicy, ExponentialRetry, FixedRetry, NoRetry
├── driver/
│   ├── driver.go          # Driver[TTx], Executor, ExecutorTx, Listener interfaces
│   ├── memory/            # in-memory driver (tests)
│   ├── pgxv5/             # PostgreSQL via pgx/v5 (LISTEN/NOTIFY, advisory locks)
│   └── stdlib/            # PostgreSQL + MySQL + SQLite via database/sql
└── internal/clock/        # Clock interface + MockClock
```

---

## License

MIT
