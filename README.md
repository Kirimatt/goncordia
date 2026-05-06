# goncordia

A job queue engine for Go that works with the database you already have.

One `Driver[TTx]` interface parameterized by your library's native transaction type covers Postgres, MySQL, SQLite, MongoDB, Redis, and in-memory — without forcing you to adopt a new dependency.

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
| In-memory | `driver/memory` | `memory.NoTx` | ✅ | no persistence; for tests |

---

## Installation

```bash
go get github.com/goncordia/goncordia
```

Pick a driver:

```bash
# PostgreSQL via pgx v5
go get github.com/goncordia/goncordia/driver/pgxv5 github.com/jackc/pgx/v5

# PostgreSQL / MySQL / SQLite via database/sql
go get github.com/goncordia/goncordia/driver/stdlib

# gorm adapter
go get github.com/goncordia/goncordia/driver/gorm gorm.io/gorm

# bun adapter
go get github.com/goncordia/goncordia/driver/bun github.com/uptrace/bun

# MongoDB (replica set required)
go get github.com/goncordia/goncordia/driver/mongodb go.mongodb.org/mongo-driver/mongo

# Redis
go get github.com/goncordia/goncordia/driver/redis github.com/redis/go-redis/v9
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
    mongodriver "github.com/goncordia/goncordia/driver/mongodb"
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
    gormdriver "github.com/goncordia/goncordia/driver/gorm"
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
    bundriver "github.com/goncordia/goncordia/driver/bun"
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
    redisdriver "github.com/goncordia/goncordia/driver/redis"
    "github.com/redis/go-redis/v9"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
d := redisdriver.New(rdb)
d.Migrate(ctx)  // pings Redis to verify connectivity

client := redisdriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, MyJob{...}, nil)

// EnqueueTx is not supported on the Redis driver:
// there is no rollback guarantee. Use Enqueue (post-commit pattern) instead.
```

### SQLite (no Docker, good for tests)

```go
import (
    _ "modernc.org/sqlite"
    stdlibdriver "github.com/goncordia/goncordia/driver/stdlib"
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
client.Enqueue(ctx, MyJobArgs{...}, &core.InsertOpts{
    Queue:    "critical",                    // override default queue
    Priority: 10,                            // higher = processed first
    RunAt:    time.Now().Add(time.Hour),     // schedule for later

    UniqueOpts: &core.UniqueOpts{            // deduplicate
        ByArgs:  true,
        ByQueue: true,
    },

    MaxRetry: intPtr(3),
    Tags:     []string{"user:42"},
})
```

---

## WorkerConfig

```go
goncordia.WorkerConfig{
    Queues:          []string{"default", "critical"},
    Concurrency:     20,
    PollInterval:    500 * time.Millisecond,  // fallback when no push notifications
    RetryPolicy:     core.ExponentialRetry{Base: time.Second, Max: time.Hour},
    ShutdownTimeout: 30 * time.Second,
    Clock:           clk,  // inject MockClock in tests
}
```

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
    "github.com/goncordia/goncordia/driver/memory"
    "github.com/goncordia/goncordia/internal/clock"
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
│   ├── memory/            # in-memory (no persistence; for tests)
│   ├── pgxv5/             # PostgreSQL via pgx v5 (LISTEN/NOTIFY, advisory locks)
│   ├── stdlib/            # PostgreSQL + MySQL + SQLite via database/sql
│   ├── gorm/              # gorm adapter (wraps stdlib)
│   ├── bun/               # bun adapter (wraps stdlib)
│   ├── mongodb/           # MongoDB 4.0+ replica set
│   └── redis/             # Redis (at-least-once; Pub/Sub notifications)
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
| In-memory | Atomic (in-process) | Single mutex |

---

## License

MIT
