// Package memory provides an in-memory driver for testing and development.
// It has no persistence — all jobs are lost on process restart.
// TTx is struct{} since in-memory operations don't need real transactions.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/goncordia/goncordia/driver"
)

// NoTx is the transaction type for the memory driver.
// In-memory "transactions" are no-ops (state is managed by locks).
type NoTx struct{}

// Driver implements driver.Driver[NoTx] using in-memory maps.
type Driver struct {
	mu     sync.Mutex
	jobs   map[string]*driver.JobRow
	queues map[string]*driver.QueueRow
	seq    uint64
	notify map[string][]chan driver.Notification
}

// New creates a new in-memory Driver.
func New() *Driver {
	return &Driver{
		jobs:   make(map[string]*driver.JobRow),
		queues: make(map[string]*driver.QueueRow),
		notify: make(map[string][]chan driver.Notification),
	}
}

func (d *Driver) Name() string { return "memory" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:      true, // trivially: lock-based
		SkipLocked:    true,
		UniqueJobs:    true,
		ListenNotify:  true,
		AdvisoryLocks: false,
	}
}

func (d *Driver) Executor() driver.Executor { return &executor{d: d} }

func (d *Driver) UnwrapTx(tx NoTx) driver.ExecutorTx { return &txExecutor{executor: executor{d: d}} }

func (d *Driver) Listener() driver.Listener { return &listener{d: d} }

func (d *Driver) Close() error { return nil }

// --- executor ---

type executor struct{ d *Driver }

func (e *executor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return &txExecutor{executor: executor{d: e.d}}, nil
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()

	results := make([]driver.JobInsertResult, 0, len(params))
	for _, p := range params {
		// Unique job check
		if p.UniqueKey != "" {
			if dup := e.d.findUniqueJob(p.Queue, p.UniqueKey); dup != nil {
				results = append(results, driver.JobInsertResult{Job: dup, UniqueSkip: true})
				continue
			}
		}

		e.d.seq++
		id := fmt.Sprintf("mem_%d", e.d.seq)
		now := time.Now()
		runAt := p.RunAt
		if runAt.IsZero() {
			runAt = now
		}
		state := driver.JobStateAvailable
		if runAt.After(now) {
			state = driver.JobStateScheduled
		}
		row := &driver.JobRow{
			ID:        id,
			Queue:     p.Queue,
			Kind:      p.Kind,
			Args:      p.Args,
			State:     state,
			Priority:  p.Priority,
			RunAt:     runAt,
			CreatedAt: now,
			MaxRetry:  p.MaxRetry,
			Timeout:   p.Timeout,
			Tags:      p.Tags,
			UniqueKey: p.UniqueKey,
		}
		e.d.jobs[id] = row
		e.d.ensureQueue(p.Queue)
		e.d.broadcastNotify(p.Queue)

		results = append(results, driver.JobInsertResult{Job: row})
	}
	return results, nil
}

func (e *executor) JobGetByID(_ context.Context, id string) (*driver.JobRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %q not found", id)
	}
	cp := *row
	return &cp, nil
}

func (e *executor) JobFetchBatch(_ context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()

	q, ok := e.d.queues[params.Queue]
	if !ok || (q != nil && q.Paused) {
		return nil, nil
	}

	now := time.Now()
	var candidates []*driver.JobRow
	for _, j := range e.d.jobs {
		if j.Queue == params.Queue &&
			j.State == driver.JobStateAvailable &&
			!j.RunAt.After(now) {
			candidates = append(candidates, j)
		}
	}

	// Sort: higher priority first, then earlier RunAt
	sort.Slice(candidates, func(i, k int) bool {
		if candidates[i].Priority != candidates[k].Priority {
			return candidates[i].Priority > candidates[k].Priority
		}
		return candidates[i].RunAt.Before(candidates[k].RunAt)
	})

	limit := params.Limit
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}

	now2 := time.Now()
	rows := make([]driver.JobRow, 0, limit)
	for _, j := range candidates[:limit] {
		j.State = driver.JobStateRunning
		j.AttemptedAt = &now2
		j.AttemptNum++
		j.WorkerID = params.WorkerID
		cp := *j
		rows = append(rows, cp)
	}
	return rows, nil
}

func (e *executor) JobSetStateIfRunning(_ context.Context, params driver.JobSetStateParams) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[params.ID]
	if !ok || row.State != driver.JobStateRunning {
		return nil
	}
	row.State = params.State
	if params.Err != nil {
		row.Errors = append(row.Errors, driver.AttemptError{
			At:      time.Now(),
			Attempt: row.AttemptNum,
			Error:   *params.Err,
		})
	}
	if params.State == driver.JobStateRetryable && !params.RetryAt.IsZero() {
		row.RunAt = params.RetryAt
		row.State = driver.JobStateAvailable
	}
	if params.State == driver.JobStateCompleted || params.State == driver.JobStateDiscarded || params.State == driver.JobStateCancelled {
		now := time.Now()
		row.FinalizedAt = &now
	}
	return nil
}

func (e *executor) JobCancel(_ context.Context, id string) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	if row.State != driver.JobStateAvailable && row.State != driver.JobStateScheduled {
		return fmt.Errorf("job %q is in state %s, can only cancel available/scheduled", id, row.State)
	}
	row.State = driver.JobStateCancelled
	now := time.Now()
	row.FinalizedAt = &now
	return nil
}

func (e *executor) JobDelete(_ context.Context, id string) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	delete(e.d.jobs, id)
	return nil
}

func (e *executor) JobReschedule(_ context.Context, params driver.RescheduleParams) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[params.ID]
	if !ok {
		return fmt.Errorf("job %q not found", params.ID)
	}
	row.RunAt = params.RunAt
	row.State = driver.JobStateScheduled
	return nil
}

func (e *executor) QueueGet(_ context.Context, name string) (*driver.QueueRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	q, ok := e.d.queues[name]
	if !ok {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	cp := *q
	return &cp, nil
}

func (e *executor) QueuePause(_ context.Context, name string) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	e.d.ensureQueue(name)
	e.d.queues[name].Paused = true
	return nil
}

func (e *executor) QueueResume(_ context.Context, name string) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	e.d.ensureQueue(name)
	e.d.queues[name].Paused = false
	return nil
}

func (e *executor) QueueList(_ context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	rows := make([]*driver.QueueRow, 0, len(e.d.queues))
	for _, q := range e.d.queues {
		cp := *q
		rows = append(rows, &cp)
	}
	return rows, nil
}

func (e *executor) LeaderAttemptElect(_ context.Context, params driver.LeaderElectParams) (bool, error) {
	// Single-process: always elected.
	return true, nil
}

func (e *executor) LeaderResign(_ context.Context, _ string) error { return nil }

// --- txExecutor wraps executor with commit/rollback no-ops ---

type txExecutor struct {
	executor
}

func (t *txExecutor) Commit(_ context.Context) error   { return nil }
func (t *txExecutor) Rollback(_ context.Context) error { return nil }

// --- listener ---

type listener struct{ d *Driver }

func (l *listener) Listen(_ context.Context, queue string) (<-chan driver.Notification, error) {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	ch := make(chan driver.Notification, 16)
	l.d.notify[queue] = append(l.d.notify[queue], ch)
	return ch, nil
}

func (l *listener) Unlisten(_ context.Context, queue string) error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	delete(l.d.notify, queue)
	return nil
}

func (l *listener) Close() error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	for _, chans := range l.d.notify {
		for _, ch := range chans {
			close(ch)
		}
	}
	l.d.notify = make(map[string][]chan driver.Notification)
	return nil
}

// --- internal helpers ---

func (d *Driver) ensureQueue(name string) {
	if _, ok := d.queues[name]; !ok {
		d.queues[name] = &driver.QueueRow{Name: name, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	}
}

func (d *Driver) broadcastNotify(queue string) {
	for _, ch := range d.notify[queue] {
		select {
		case ch <- driver.Notification{Queue: queue}:
		default:
		}
	}
}

func (d *Driver) findUniqueJob(queue, uniqueKey string) *driver.JobRow {
	for _, j := range d.jobs {
		if j.Queue == queue && j.UniqueKey == uniqueKey &&
			j.State != driver.JobStateCompleted &&
			j.State != driver.JobStateDiscarded &&
			j.State != driver.JobStateCancelled {
			return j
		}
	}
	return nil
}

// Ensure Driver satisfies the interface at compile time.
var _ driver.Driver[NoTx] = (*Driver)(nil)

// Ensure executor satisfies the interface at compile time.
var _ driver.Executor = (*executor)(nil)

// Ensure json import is used (for future use or clarity)
var _ = json.Marshal
