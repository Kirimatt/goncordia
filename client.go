// Package goncordia provides a transactional job queue engine for Go.
// It supports multiple storage backends (Postgres, MySQL, SQLite, MongoDB, Redis, in-memory)
// through a driver interface parameterized by the native transaction type of each backend.
//
// Transactional usage (shared transaction with business logic):
//
//	tx, _ := pool.Begin(ctx)
//	_, _ = queries.CreateOrder(ctx, tx, orderParams)
//	_, _ = client.EnqueueTx(ctx, tx, SendConfirmationEmailArgs{OrderID: id}, nil)
//	tx.Commit(ctx) // both operations are atomic
//
// Non-transactional usage (at-least-once semantics):
//
//	client.Enqueue(ctx, SendConfirmationEmailArgs{OrderID: id}, nil)
package goncordia

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/goncordia/goncordia/core"
	"github.com/goncordia/goncordia/driver"
)

// Client enqueues jobs into the job queue.
// TTx is the transaction type of the chosen backend driver
// (e.g. *pgx.Tx for pgxv5, *sql.Tx for stdlib, mongo.SessionContext for mongodb).
type Client[TTx any] struct {
	driver driver.Driver[TTx]
	config ClientConfig
}

// ClientConfig controls optional Client behavior.
type ClientConfig struct {
	// DefaultQueue is used when InsertOpts.Queue is empty. Default: "default".
	DefaultQueue string
}

// NewClient creates a Client backed by the given driver.
func NewClient[TTx any](d driver.Driver[TTx], cfg ClientConfig) *Client[TTx] {
	if cfg.DefaultQueue == "" {
		cfg.DefaultQueue = "default"
	}
	return &Client[TTx]{driver: d, config: cfg}
}

// Enqueue inserts a single job without a transaction (at-least-once semantics).
// Safe to call for all backends; for SQL/MongoDB backends prefer EnqueueTx for atomicity.
func (c *Client[TTx]) Enqueue(ctx context.Context, args core.JobArgs, opts *core.InsertOpts) (*driver.JobInsertResult, error) {
	params, err := c.buildInsertParams(args, opts)
	if err != nil {
		return nil, err
	}
	results, err := c.driver.Executor().JobInsertMany(ctx, []driver.JobInsertParams{params})
	if err != nil {
		return nil, err
	}
	return &results[0], nil
}

// EnqueueTx inserts a job within an existing transaction.
// The job becomes visible to workers only when tx is committed.
// Only available on backends with Capabilities.NativeTx == true.
func (c *Client[TTx]) EnqueueTx(ctx context.Context, tx TTx, args core.JobArgs, opts *core.InsertOpts) (*driver.JobInsertResult, error) {
	if !c.driver.Capabilities().NativeTx {
		return nil, fmt.Errorf("driver %q does not support transactional inserts", c.driver.Name())
	}
	params, err := c.buildInsertParams(args, opts)
	if err != nil {
		return nil, err
	}
	etx := c.driver.UnwrapTx(tx)
	results, err := etx.JobInsertMany(ctx, []driver.JobInsertParams{params})
	if err != nil {
		return nil, err
	}
	return &results[0], nil
}

// EnqueueMany inserts multiple jobs in a single batch (non-transactional).
func (c *Client[TTx]) EnqueueMany(ctx context.Context, args []core.JobArgs, opts *core.InsertOpts) ([]driver.JobInsertResult, error) {
	params := make([]driver.JobInsertParams, 0, len(args))
	for _, a := range args {
		p, err := c.buildInsertParams(a, opts)
		if err != nil {
			return nil, err
		}
		params = append(params, p)
	}
	return c.driver.Executor().JobInsertMany(ctx, params)
}

// EnqueueManyTx inserts multiple jobs within an existing transaction.
func (c *Client[TTx]) EnqueueManyTx(ctx context.Context, tx TTx, args []core.JobArgs, opts *core.InsertOpts) ([]driver.JobInsertResult, error) {
	if !c.driver.Capabilities().NativeTx {
		return nil, fmt.Errorf("driver %q does not support transactional inserts", c.driver.Name())
	}
	params := make([]driver.JobInsertParams, 0, len(args))
	for _, a := range args {
		p, err := c.buildInsertParams(a, opts)
		if err != nil {
			return nil, err
		}
		params = append(params, p)
	}
	etx := c.driver.UnwrapTx(tx)
	return etx.JobInsertMany(ctx, params)
}

// Cancel marks a job as cancelled. The job must be in available or scheduled state.
func (c *Client[TTx]) Cancel(ctx context.Context, id string) error {
	return c.driver.Executor().JobCancel(ctx, id)
}

func (c *Client[TTx]) buildInsertParams(args core.JobArgs, opts *core.InsertOpts) (driver.JobInsertParams, error) {
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return driver.JobInsertParams{}, fmt.Errorf("marshal job args: %w", err)
	}

	queue := c.config.DefaultQueue
	var priority int
	var runAt time.Time
	var uniqueKey string
	var maxRetry int
	var timeout time.Duration
	var tags []string

	if opts != nil {
		if opts.Queue != "" {
			queue = opts.Queue
		}
		priority = opts.Priority
		runAt = opts.RunAt
		maxRetry = func() int {
			if opts.MaxRetry != nil {
				return *opts.MaxRetry
			}
			return 0
		}()
		timeout = func() time.Duration {
			if opts.Timeout != nil {
				return *opts.Timeout
			}
			return 0
		}()
		tags = opts.Tags

		if opts.UniqueOpts != nil {
			uniqueKey, err = buildUniqueKey(args, queue, opts.UniqueOpts)
			if err != nil {
				return driver.JobInsertParams{}, err
			}
		}
	}

	return driver.JobInsertParams{
		Queue:     queue,
		Kind:      args.Kind(),
		Args:      argsJSON,
		Priority:  priority,
		RunAt:     runAt,
		UniqueKey: uniqueKey,
		MaxRetry:  maxRetry,
		Timeout:   timeout,
		Tags:      tags,
	}, nil
}

func buildUniqueKey(args core.JobArgs, queue string, opts *core.UniqueOpts) (string, error) {
	if opts == nil {
		return "", nil
	}

	key := args.Kind()
	if opts.ByQueue {
		key += ":" + queue
	}
	if opts.ByArgs {
		b, err := json.Marshal(args)
		if err != nil {
			return "", fmt.Errorf("marshal args for unique key: %w", err)
		}
		key += ":" + string(b)
	}
	if opts.ByPeriod > 0 {
		window := time.Now().Truncate(opts.ByPeriod)
		key += ":" + window.Format(time.RFC3339)
	}
	return key, nil
}
