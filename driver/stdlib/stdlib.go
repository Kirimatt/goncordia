// Package stdlib provides a goncordia driver backed by database/sql.
// Supports PostgreSQL, MySQL, and SQLite via a Dialect switch.
//
// Usage (PostgreSQL via pgx stdlib adapter):
//
//	import _ "github.com/jackc/pgx/v5/stdlib"
//	db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
//	d, _ := stdlib.New(db, stdlib.Postgres)
//
// Usage (SQLite, no Docker needed):
//
//	import _ "modernc.org/sqlite"
//	db, _ := sql.Open("sqlite", "./jobs.db")
//	d, _ := stdlib.New(db, stdlib.SQLite)
package stdlib

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"

	goncordia "github.com/goncordia/goncordia"
	"github.com/goncordia/goncordia/core"
	"github.com/goncordia/goncordia/driver"
	"github.com/goncordia/goncordia/internal/clock"
)

//go:embed migrations/**/*.sql
var migrationFS embed.FS

// Dialect identifies the SQL dialect of the underlying database.
type Dialect int

const (
	Postgres Dialect = iota
	MySQL
	SQLite
)

func (d Dialect) String() string {
	switch d {
	case Postgres:
		return "postgres"
	case MySQL:
		return "mysql"
	case SQLite:
		return "sqlite"
	default:
		return "unknown"
	}
}

// placeholder returns the positional placeholder for argument n (1-based).
// Postgres uses $1, MySQL/SQLite use ?.
func (d Dialect) placeholder(n int) string {
	if d == Postgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

// placeholders returns a comma-separated list of n placeholders starting at offset.
func (d Dialect) placeholders(n, offset int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = d.placeholder(i + offset)
	}
	return strings.Join(parts, ", ")
}

// supportsSkipLocked reports whether this dialect supports SELECT FOR UPDATE SKIP LOCKED.
func (d Dialect) supportsSkipLocked() bool {
	return d == Postgres || d == MySQL
}

// Driver implements driver.Driver[*sql.Tx] backed by database/sql.
type Driver struct {
	db      *sql.DB
	dialect Dialect
	clk     clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (for testing).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver from an existing *sql.DB.
// Call Migrate to create the schema before starting workers.
func New(db *sql.DB, dialect Dialect, opts ...Option) *Driver {
	d := &Driver{db: db, dialect: dialect, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate runs embedded SQL migrations for the configured dialect.
func (d *Driver) Migrate(ctx context.Context) error {
	dir := "migrations/" + d.dialect.String()
	entries, err := migrationFS.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations for %s: %w", d.dialect, err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		sqlBytes, err := migrationFS.ReadFile(dir + "/" + e.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		// Split on semicolons to run each statement individually
		for _, stmt := range splitStatements(string(sqlBytes)) {
			if _, err := d.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("apply migration %s: %w", e.Name(), err)
			}
		}
	}
	return nil
}

func (d *Driver) Name() string { return d.dialect.String() }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:      true,
		SkipLocked:    d.dialect.supportsSkipLocked(),
		UniqueJobs:    true,
		ListenNotify:  false, // stdlib doesn't support LISTEN/NOTIFY
		AdvisoryLocks: d.dialect == Postgres,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{db: d.db, dialect: d.dialect, clk: d.clk}
}

func (d *Driver) UnwrapTx(tx *sql.Tx) driver.ExecutorTx {
	return &txExecutor{tx: tx, dialect: d.dialect, clk: d.clk}
}

// Listener returns nil — stdlib driver uses polling, not push notifications.
func (d *Driver) Listener() driver.Listener { return nil }

func (d *Driver) Close() error { return d.db.Close() }

// splitStatements splits a SQL file into individual statements on semicolons,
// skipping empty/whitespace-only statements.
func splitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	var result []string
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

// Client is a type alias so callers never write goncordia.Client[*sql.Tx].
type Client = goncordia.Client[*sql.Tx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[*sql.Tx].
type WorkerPool = goncordia.WorkerPool[*sql.Tx]

// NewClient creates a Client bound to this stdlib driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[*sql.Tx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this stdlib driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[*sql.Tx](d, r, cfg)
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

// DB returns the underlying *sql.DB (e.g. for opening transactions in tests).
func (d *Driver) DB() *sql.DB { return d.db }

// compile-time check
var _ driver.Driver[*sql.Tx] = (*Driver)(nil)
