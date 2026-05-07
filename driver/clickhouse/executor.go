package clickhousedriver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/internal/clock"
)

// ---- executor ----

type executor struct {
	conn chdriver.Conn
	clk  clock.Clock
}

func (e *executor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return &txExecutor{executor: *e}, nil
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.conn, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.conn, id)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.conn, e.clk, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.conn, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.conn, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.conn, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.conn, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.conn, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.conn, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.conn, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.conn, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.conn, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, e.conn, name)
}

// ---- txExecutor (no-op tx — ClickHouse has no transactions) ----

type txExecutor struct{ executor }

func (t *txExecutor) Commit(_ context.Context) error   { return nil }
func (t *txExecutor) Rollback(_ context.Context) error { return nil }
func (t *txExecutor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}
func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, t.conn, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, t.conn, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, t.conn, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, t.conn, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, t.conn, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, t.conn, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, t.conn, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, t.conn, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.conn, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.conn, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, t.conn, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, t.conn, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, t.conn, name)
}

// ---- job row helpers ----

type storedError struct {
	At      int64  `json:"at_ms"`
	Attempt int    `json:"attempt"`
	Error   string `json:"error"`
}

func marshalErrors(errs []driver.AttemptError) string {
	if len(errs) == 0 {
		return "[]"
	}
	out := make([]storedError, len(errs))
	for i, e := range errs {
		out[i] = storedError{At: e.At.UnixMilli(), Attempt: e.Attempt, Error: e.Error}
	}
	b, _ := json.Marshal(out)
	return string(b)
}

func unmarshalErrors(s string) []driver.AttemptError {
	if s == "" || s == "[]" {
		return nil
	}
	var stored []storedError
	if err := json.Unmarshal([]byte(s), &stored); err != nil {
		return nil
	}
	out := make([]driver.AttemptError, len(stored))
	for i, e := range stored {
		out[i] = driver.AttemptError{At: time.UnixMilli(e.At).UTC(), Attempt: e.Attempt, Error: e.Error}
	}
	return out
}

// chJob is the scan target for SELECT queries.
type chJob struct {
	ID          string
	Queue       string
	Kind        string
	Args        string
	State       string
	Priority    int32
	RunAt       time.Time
	CreatedAt   time.Time
	AttemptedAt *time.Time
	FinalizedAt *time.Time
	AttemptNum  int32
	MaxRetry    int32
	TimeoutMs   int64
	UniqueKey   string
	WorkerID    string
	Tags        []string
	ErrorsJSON  string
	Version     int64
}

func (j chJob) toRow() *driver.JobRow {
	row := &driver.JobRow{
		ID:          j.ID,
		Queue:       j.Queue,
		Kind:        j.Kind,
		Args:        []byte(j.Args),
		State:       driver.JobState(j.State),
		Priority:    int(j.Priority),
		RunAt:       j.RunAt.UTC(),
		CreatedAt:   j.CreatedAt.UTC(),
		AttemptedAt: j.AttemptedAt,
		FinalizedAt: j.FinalizedAt,
		AttemptNum:  int(j.AttemptNum),
		MaxRetry:    int(j.MaxRetry),
		Timeout:     time.Duration(j.TimeoutMs) * time.Millisecond,
		UniqueKey:   j.UniqueKey,
		WorkerID:    j.WorkerID,
		Tags:        j.Tags,
		Errors:      unmarshalErrors(j.ErrorsJSON),
	}
	return row
}

const selectJobCols = `id, queue, kind, args, state, priority, run_at, created_at,
	attempted_at, finalized_at, attempt_num, max_retry, timeout_ms,
	unique_key, worker_id, tags, errors_json, version`

func scanJob(row chdriver.Row) (chJob, error) {
	var j chJob
	err := row.Scan(
		&j.ID, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority,
		&j.RunAt, &j.CreatedAt, &j.AttemptedAt, &j.FinalizedAt,
		&j.AttemptNum, &j.MaxRetry, &j.TimeoutMs,
		&j.UniqueKey, &j.WorkerID, &j.Tags, &j.ErrorsJSON, &j.Version,
	)
	return j, err
}

func queryJob(ctx context.Context, conn chdriver.Conn, id string) (chJob, error) {
	row := conn.QueryRow(ctx,
		`SELECT `+selectJobCols+` FROM goncordia_jobs FINAL WHERE id = ?`, id)
	return scanJob(row)
}

func insertJobRow(ctx context.Context, conn chdriver.Conn, j chJob) error {
	return conn.Exec(ctx,
		`INSERT INTO goncordia_jobs
		(id,queue,kind,args,state,priority,run_at,created_at,
		 attempted_at,finalized_at,attempt_num,max_retry,timeout_ms,
		 unique_key,worker_id,tags,errors_json,version)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.Queue, j.Kind, j.Args, j.State, j.Priority,
		j.RunAt, j.CreatedAt, j.AttemptedAt, j.FinalizedAt,
		j.AttemptNum, j.MaxRetry, j.TimeoutMs,
		j.UniqueKey, j.WorkerID, j.Tags, j.ErrorsJSON, j.Version,
	)
}

// ---- JobInsertMany ----

func jobInsertMany(ctx context.Context, conn chdriver.Conn, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	now := clk.Now()
	results := make([]driver.JobInsertResult, len(params))

	for i, p := range params {
		runAt := p.RunAt
		if runAt.IsZero() {
			runAt = now
		}
		state := driver.JobStateAvailable
		if runAt.After(now) {
			state = driver.JobStateScheduled
		}

		id := uuid.New().String()
		tags := p.Tags
		if tags == nil {
			tags = []string{}
		}

		// Soft unique-key check: look for an existing available/running job with this key.
		if p.UniqueKey != "" {
			var existing string
			row := conn.QueryRow(ctx,
				`SELECT id FROM goncordia_jobs FINAL
				 WHERE queue=? AND unique_key=? AND state IN ('available','running','retryable','scheduled')
				 LIMIT 1`,
				p.Queue, p.UniqueKey)
			if err := row.Scan(&existing); err == nil && existing != "" {
				results[i] = driver.JobInsertResult{UniqueSkip: true}
				continue
			}
		}

		j := chJob{
			ID:         id,
			Queue:      p.Queue,
			Kind:       p.Kind,
			Args:       string(p.Args),
			State:      string(state),
			Priority:   int32(p.Priority),
			RunAt:      runAt,
			CreatedAt:  now,
			AttemptNum: 0,
			MaxRetry:   int32(p.MaxRetry),
			TimeoutMs:  p.Timeout.Milliseconds(),
			UniqueKey:  p.UniqueKey,
			Tags:       tags,
			ErrorsJSON: "[]",
			Version:    1,
		}

		if err := insertJobRow(ctx, conn, j); err != nil {
			return nil, fmt.Errorf("insert job: %w", err)
		}

		// Upsert queue metadata (higher version wins in ReplacingMergeTree).
		if err := upsertQueue(ctx, conn, clk, p.Queue); err != nil {
			return nil, err
		}

		results[i] = driver.JobInsertResult{Job: j.toRow()}
	}
	return results, nil
}

// ---- JobGetByID ----

func jobGetByID(ctx context.Context, conn chdriver.Conn, id string) (*driver.JobRow, error) {
	j, err := queryJob(ctx, conn, id)
	if err != nil {
		return nil, nil // not found or error — treat as not found
	}
	if j.ID == "" {
		return nil, nil
	}
	return j.toRow(), nil
}

// ---- JobFetchBatch ----

// JobFetchBatch finds available jobs and attempts to claim them by inserting a
// higher-version row. It then re-reads with FINAL to confirm ownership.
func jobFetchBatch(ctx context.Context, conn chdriver.Conn, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	paused, err := isQueuePaused(ctx, conn, params.Queue)
	if err != nil {
		return nil, err
	}
	if paused {
		return nil, nil
	}

	now := clk.Now()

	// Find available candidates.
	rows, err := conn.Query(ctx,
		`SELECT `+selectJobCols+` FROM goncordia_jobs FINAL
		 WHERE queue=? AND state='available' AND run_at<=?
		 ORDER BY priority DESC, run_at ASC
		 LIMIT ?`,
		params.Queue, now, params.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch candidates: %w", err)
	}

	var candidates []chJob
	for rows.Next() {
		var j chJob
		if err := rows.Scan(
			&j.ID, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority,
			&j.RunAt, &j.CreatedAt, &j.AttemptedAt, &j.FinalizedAt,
			&j.AttemptNum, &j.MaxRetry, &j.TimeoutMs,
			&j.UniqueKey, &j.WorkerID, &j.Tags, &j.ErrorsJSON, &j.Version,
		); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	// Claim each candidate by inserting a higher-version row.
	var claimedIDs []string
	for _, c := range candidates {
		claimed := c
		claimed.State = string(driver.JobStateRunning)
		claimed.WorkerID = params.WorkerID
		claimed.AttemptedAt = &now
		claimed.AttemptNum = c.AttemptNum + 1
		claimed.Version = c.Version + 1

		if err := insertJobRow(ctx, conn, claimed); err != nil {
			return nil, fmt.Errorf("claim insert: %w", err)
		}
		claimedIDs = append(claimedIDs, c.ID)
	}

	// Re-read with FINAL to confirm we own the jobs (highest version wins).
	placeholders := make([]string, len(claimedIDs))
	args := make([]any, len(claimedIDs)+1)
	args[0] = params.WorkerID
	for i, id := range claimedIDs {
		placeholders[i] = "?"
		args[i+1] = id
	}

	confirmRows, err := conn.Query(ctx,
		`SELECT `+selectJobCols+` FROM goncordia_jobs FINAL
		 WHERE worker_id=? AND id IN (`+strings.Join(placeholders, ",")+`)
		 AND state='running'`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("confirm claims: %w", err)
	}
	defer confirmRows.Close()

	var result []driver.JobRow
	for confirmRows.Next() {
		var j chJob
		if err := confirmRows.Scan(
			&j.ID, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority,
			&j.RunAt, &j.CreatedAt, &j.AttemptedAt, &j.FinalizedAt,
			&j.AttemptNum, &j.MaxRetry, &j.TimeoutMs,
			&j.UniqueKey, &j.WorkerID, &j.Tags, &j.ErrorsJSON, &j.Version,
		); err != nil {
			return nil, err
		}
		result = append(result, *j.toRow())
	}
	return result, confirmRows.Err()
}

// ---- JobSetStateIfRunning ----

func jobSetStateIfRunning(ctx context.Context, conn chdriver.Conn, clk clock.Clock, params driver.JobSetStateParams) error {
	j, err := queryJob(ctx, conn, params.ID)
	if err != nil || j.ID == "" {
		return nil
	}
	if j.State != string(driver.JobStateRunning) {
		return nil
	}

	now := clk.Now()
	errs := unmarshalErrors(j.ErrorsJSON)
	if params.Err != nil {
		errs = append(errs, driver.AttemptError{At: now, Attempt: int(j.AttemptNum), Error: *params.Err})
	}

	updated := j
	updated.ErrorsJSON = marshalErrors(errs)
	updated.Version = j.Version + 1

	if params.State == driver.JobStateRetryable {
		retryAt := params.RetryAt
		if retryAt.IsZero() {
			retryAt = now
		}
		updated.State = string(driver.JobStateAvailable)
		updated.RunAt = retryAt
		updated.WorkerID = ""
	} else {
		updated.State = string(params.State)
		updated.FinalizedAt = &now
		updated.WorkerID = ""
	}

	return insertJobRow(ctx, conn, updated)
}

// ---- JobCancel ----

func jobCancel(ctx context.Context, conn chdriver.Conn, clk clock.Clock, id string) error {
	j, err := queryJob(ctx, conn, id)
	if err != nil || j.ID == "" {
		return fmt.Errorf("job %q not found", id)
	}
	if j.State != string(driver.JobStateAvailable) && j.State != string(driver.JobStateScheduled) {
		return fmt.Errorf("job %q is in state %s, can only cancel available/scheduled", id, j.State)
	}

	now := clk.Now()
	cancelled := j
	cancelled.State = string(driver.JobStateCancelled)
	cancelled.FinalizedAt = &now
	cancelled.Version = j.Version + 1
	return insertJobRow(ctx, conn, cancelled)
}

// ---- JobDelete ----

func jobDelete(ctx context.Context, conn chdriver.Conn, id string) error {
	// ClickHouse doesn't support row-level deletes on MergeTree tables in the same
	// way as OLTP databases. We mark the job as deleted by setting a terminal state.
	// Physical deletion happens via TTL or ALTER TABLE DELETE (async mutation).
	j, err := queryJob(ctx, conn, id)
	if err != nil || j.ID == "" {
		return nil
	}
	deleted := j
	deleted.State = "deleted"
	deleted.Version = j.Version + 1
	return insertJobRow(ctx, conn, deleted)
}

// ---- JobReschedule ----

func jobReschedule(ctx context.Context, conn chdriver.Conn, params driver.RescheduleParams) error {
	j, err := queryJob(ctx, conn, params.ID)
	if err != nil || j.ID == "" {
		return fmt.Errorf("job %q not found", params.ID)
	}
	rescheduled := j
	rescheduled.State = string(driver.JobStateScheduled)
	rescheduled.RunAt = params.RunAt
	rescheduled.Version = j.Version + 1
	return insertJobRow(ctx, conn, rescheduled)
}

// ---- Queue ----

func upsertQueue(ctx context.Context, conn chdriver.Conn, clk clock.Clock, name string) error {
	now := clk.Now()
	// Check if exists first to avoid bumping the version on every insert.
	var existing string
	row := conn.QueryRow(ctx, `SELECT name FROM goncordia_queues FINAL WHERE name=? LIMIT 1`, name)
	if err := row.Scan(&existing); err == nil && existing != "" {
		return nil
	}
	return conn.Exec(ctx,
		`INSERT INTO goncordia_queues (name, paused, created_at, updated_at, version) VALUES (?,0,?,?,1)`,
		name, now, now,
	)
}

func isQueuePaused(ctx context.Context, conn chdriver.Conn, name string) (bool, error) {
	var paused uint8
	row := conn.QueryRow(ctx, `SELECT paused FROM goncordia_queues FINAL WHERE name=? LIMIT 1`, name)
	if err := row.Scan(&paused); err != nil {
		return false, nil // queue doesn't exist yet → not paused
	}
	return paused == 1, nil
}

func queueGet(ctx context.Context, conn chdriver.Conn, name string) (*driver.QueueRow, error) {
	var q driver.QueueRow
	var paused uint8
	row := conn.QueryRow(ctx,
		`SELECT name, paused, created_at, updated_at FROM goncordia_queues FINAL WHERE name=? LIMIT 1`, name)
	if err := row.Scan(&q.Name, &paused, &q.CreatedAt, &q.UpdatedAt); err != nil {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	q.Paused = paused == 1
	return &q, nil
}

func queueSetPaused(ctx context.Context, conn chdriver.Conn, clk clock.Clock, name string, paused bool) error {
	now := clk.Now()
	var ver int64
	row := conn.QueryRow(ctx, `SELECT version FROM goncordia_queues FINAL WHERE name=? LIMIT 1`, name)
	_ = row.Scan(&ver)
	p := uint8(0)
	if paused {
		p = 1
	}
	return conn.Exec(ctx,
		`INSERT INTO goncordia_queues (name, paused, created_at, updated_at, version) VALUES (?,?,now(),?,?)`,
		name, p, now, ver+1,
	)
}

func queueList(ctx context.Context, conn chdriver.Conn, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	rows, err := conn.Query(ctx,
		`SELECT name, paused, created_at, updated_at FROM goncordia_queues FINAL LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*driver.QueueRow
	for rows.Next() {
		var q driver.QueueRow
		var paused uint8
		if err := rows.Scan(&q.Name, &paused, &q.CreatedAt, &q.UpdatedAt); err != nil {
			return nil, err
		}
		q.Paused = paused == 1
		result = append(result, &q)
	}
	return result, rows.Err()
}

// ---- Leader election ----

func leaderAttemptElect(ctx context.Context, conn chdriver.Conn, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	now := clk.Now()

	var currentWorker string
	var currentExpiry time.Time
	var currentVersion int64

	row := conn.QueryRow(ctx,
		`SELECT worker_id, expires_at, version FROM goncordia_leaders FINAL WHERE name=? LIMIT 1`,
		params.Name)
	err := row.Scan(&currentWorker, &currentExpiry, &currentVersion)

	notFound := (err != nil)
	expired := !notFound && now.After(currentExpiry)

	if notFound || expired || currentWorker == params.WorkerID {
		expiresAt := now.Add(params.TTL)
		return true, conn.Exec(ctx,
			`INSERT INTO goncordia_leaders (name, worker_id, expires_at, version) VALUES (?,?,?,?)`,
			params.Name, params.WorkerID, expiresAt, currentVersion+1,
		)
	}
	return false, nil
}

func leaderResign(ctx context.Context, conn chdriver.Conn, name string) error {
	var ver int64
	row := conn.QueryRow(ctx, `SELECT version FROM goncordia_leaders FINAL WHERE name=? LIMIT 1`, name)
	_ = row.Scan(&ver)
	past := time.Unix(0, 0)
	return conn.Exec(ctx,
		`INSERT INTO goncordia_leaders (name, worker_id, expires_at, version) VALUES (?,'',?,?)`,
		name, past, ver+1,
	)
}

// compile-time checks
var _ driver.Executor = (*executor)(nil)
var _ driver.ExecutorTx = (*txExecutor)(nil)
