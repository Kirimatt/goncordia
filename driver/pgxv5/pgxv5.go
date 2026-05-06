// Package pgxv5 provides a goncordia driver backed by PostgreSQL via pgx/v5.
//
// Usage:
//
//	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
//	d := pgxv5.New(pool)
//	client := pgxv5.NewClient(d, goncordia.ClientConfig{})  // no type parameter needed
//
//	// Transactional insert — atomic with your business logic:
//	tx, _ := pool.Begin(ctx)
//	_, _ = client.EnqueueTx(ctx, tx, SendEmailArgs{To: "..."}, nil)
//	tx.Commit(ctx)
package pgxv5

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	goncordia "github.com/goncordia/goncordia"
	"github.com/goncordia/goncordia/core"
	"github.com/goncordia/goncordia/driver"
	"github.com/goncordia/goncordia/internal/clock"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Driver implements driver.Driver[pgx.Tx] backed by a pgxpool.Pool.
type Driver struct {
	pool *pgxpool.Pool
	clk  clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (for testing).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver from an existing pgxpool.Pool.
// Call Migrate to create the schema before starting workers.
func New(pool *pgxpool.Pool, opts ...Option) *Driver {
	d := &Driver{pool: pool, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate runs embedded SQL migrations against the database.
// Safe to call multiple times (uses IF NOT EXISTS / CREATE OR REPLACE).
func (d *Driver) Migrate(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		sql, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		if _, err := d.pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply migration %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (d *Driver) Name() string { return "postgres" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:      true,
		ListenNotify:  true,
		SkipLocked:    true,
		UniqueJobs:    true,
		AdvisoryLocks: true,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{pool: d.pool, clk: d.clk}
}

// UnwrapTx converts the user's pgx.Tx into an ExecutorTx.
func (d *Driver) UnwrapTx(tx pgx.Tx) driver.ExecutorTx {
	return &txExecutor{querier: tx, clk: d.clk}
}

func (d *Driver) Listener() driver.Listener {
	return &listener{pool: d.pool}
}

func (d *Driver) Close() error {
	d.pool.Close()
	return nil
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

// Client is a type alias so callers never write goncordia.Client[pgx.Tx].
type Client = goncordia.Client[pgx.Tx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[pgx.Tx].
type WorkerPool = goncordia.WorkerPool[pgx.Tx]

// NewClient creates a Client bound to this pgxv5 driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[pgx.Tx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this pgxv5 driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[pgx.Tx](d, r, cfg)
}
