package stdlib

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goncordia/goncordia/driver"
	"github.com/goncordia/goncordia/internal/clock"
)

// executor is the non-transactional executor (uses *sql.DB).
type executor struct {
	db      *sql.DB
	dialect Dialect
	clk     clock.Clock
}

func (e *executor) Begin(ctx context.Context) (driver.ExecutorTx, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &txExecutor{tx: tx, dialect: e.dialect, clk: e.clk}, nil
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.db, e.dialect, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.db, e.dialect, id)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.db, e.dialect, e.clk, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.db, e.dialect, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.db, e.dialect, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.db, e.dialect, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.db, e.dialect, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.db, e.dialect, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.db, e.dialect, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.db, e.dialect, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.db, e.dialect, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.db, e.dialect, params)
}
func (e *executor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, e.db, e.dialect, name)
}

// txExecutor wraps *sql.Tx.
type txExecutor struct {
	tx      *sql.Tx
	dialect Dialect
	clk     clock.Clock
}

func (t *txExecutor) Commit(ctx context.Context) error   { return t.tx.Commit() }
func (t *txExecutor) Rollback(ctx context.Context) error { return t.tx.Rollback() }

func (t *txExecutor) Begin(ctx context.Context) (driver.ExecutorTx, error) {
	// database/sql doesn't support nested transactions natively — wrap in savepoint for Postgres
	if t.dialect == Postgres {
		if _, err := t.tx.ExecContext(ctx, "SAVEPOINT goncordia_sp"); err != nil {
			return nil, err
		}
		return &savepointExecutor{txExecutor: t, ctx: ctx}, nil
	}
	return t, nil // MySQL/SQLite: reuse same tx (flat)
}

func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, t.tx, t.dialect, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, t.tx, t.dialect, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, t.tx, t.dialect, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, t.tx, t.dialect, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, t.tx, t.dialect, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, t.tx, t.dialect, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, t.tx, t.dialect, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, t.tx, t.dialect, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.tx, t.dialect, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.tx, t.dialect, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, t.tx, t.dialect, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, t.tx, t.dialect, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, t.tx, t.dialect, name)
}

// savepointExecutor implements nested tx via SAVEPOINT for Postgres.
type savepointExecutor struct {
	*txExecutor
	ctx context.Context
}

func (s *savepointExecutor) Commit(_ context.Context) error {
	_, err := s.txExecutor.tx.ExecContext(s.ctx, "RELEASE SAVEPOINT goncordia_sp")
	return err
}
func (s *savepointExecutor) Rollback(_ context.Context) error {
	_, err := s.txExecutor.tx.ExecContext(s.ctx, "ROLLBACK TO SAVEPOINT goncordia_sp")
	return err
}

// --- querier interface satisfied by *sql.DB and *sql.Tx ---

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// --- SQL implementations ---

func jobInsertMany(ctx context.Context, q querier, d Dialect, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	results := make([]driver.JobInsertResult, 0, len(params))
	now := clk.Now()

	for _, p := range params {
		runAt := p.RunAt
		if runAt.IsZero() {
			runAt = now
		}
		state := driver.JobStateAvailable
		if runAt.After(now) {
			state = driver.JobStateScheduled
		}
		tags := p.Tags
		if tags == nil {
			tags = []string{}
		}
		tagsJSON, _ := json.Marshal(tags)

		var uniqueKey *string
		if p.UniqueKey != "" {
			uniqueKey = &p.UniqueKey
		}

		// Check for existing unique job
		if uniqueKey != nil {
			existing, err := findUniqueJob(ctx, q, d, p.Queue, *uniqueKey)
			if err != nil {
				return nil, err
			}
			if existing != nil {
				results = append(results, driver.JobInsertResult{Job: existing, UniqueSkip: true})
				continue
			}
		}

		args := p.Args
		if args == nil {
			args = []byte("{}")
		}

		// pgx/v5/stdlib sends []byte as bytea, not jsonb — pass JSON columns as string for Postgres.
		var argsArg, tagsArg, errorsArg interface{}
		if d == Postgres {
			argsArg = string(args)
			tagsArg = string(tagsJSON)
			errorsArg = "[]"
		} else {
			argsArg = args
			tagsArg = tagsJSON
			errorsArg = []byte("[]")
		}

		sqlArgs := []any{
			p.Queue, p.Kind, argsArg, string(state), p.Priority,
			runAt, now, p.MaxRetry, p.Timeout.Milliseconds(),
			uniqueKey, tagsArg, errorsArg,
		}

		var (
			row    *driver.JobRow
			rowErr error
		)
		if d == Postgres {
			// Postgres: LastInsertId unsupported; use RETURNING.
			insertSQL := fmt.Sprintf(`
INSERT INTO goncordia_jobs
    (queue, kind, args, state, priority, run_at, created_at, max_retry, timeout_ms, unique_key, tags, errors)
VALUES (%s)
RETURNING id`, d.placeholders(12, 1))
			var insertedID string
			if scanErr := q.QueryRowContext(ctx, insertSQL, sqlArgs...).Scan(&insertedID); scanErr != nil {
				return nil, fmt.Errorf("insert job: %w", scanErr)
			}
			row, rowErr = jobGetByID(ctx, q, d, insertedID)
		} else {
			insertSQL := fmt.Sprintf(`
INSERT INTO goncordia_jobs
    (queue, kind, args, state, priority, run_at, created_at, max_retry, timeout_ms, unique_key, tags, errors)
VALUES (%s)`, d.placeholders(12, 1))
			res, execErr := q.ExecContext(ctx, insertSQL, sqlArgs...)
			if execErr != nil {
				return nil, fmt.Errorf("insert job: %w", execErr)
			}
			id, idErr := res.LastInsertId()
			if idErr != nil {
				return nil, fmt.Errorf("get last insert id: %w", idErr)
			}
			row, rowErr = jobGetByID(ctx, q, d, strconv.FormatInt(id, 10))
		}
		if rowErr != nil {
			return nil, rowErr
		}
		results = append(results, driver.JobInsertResult{Job: row})
	}
	return results, nil
}

func findUniqueJob(ctx context.Context, q querier, d Dialect, queue, uniqueKey string) (*driver.JobRow, error) {
	query := fmt.Sprintf(`
SELECT id, queue, kind, args, state, priority, run_at, created_at,
       attempted_at, finalized_at, attempt_num, max_retry, timeout_ms,
       unique_key, worker_id, tags, errors
FROM goncordia_jobs
WHERE queue = %s AND unique_key = %s
  AND state IN ('available', 'running', 'scheduled', 'retryable')
LIMIT 1`, d.placeholder(1), d.placeholder(2))
	row := q.QueryRowContext(ctx, query, queue, uniqueKey)
	j, err := scanJobRow(d, row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return j, err
}

func jobGetByID(ctx context.Context, q querier, d Dialect, id string) (*driver.JobRow, error) {
	query := fmt.Sprintf(`
SELECT id, queue, kind, args, state, priority, run_at, created_at,
       attempted_at, finalized_at, attempt_num, max_retry, timeout_ms,
       unique_key, worker_id, tags, errors
FROM goncordia_jobs WHERE id = %s`, d.placeholder(1))
	row := q.QueryRowContext(ctx, query, id)
	return scanJobRow(d, row)
}

func jobFetchBatch(ctx context.Context, q querier, d Dialect, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	if params.Limit <= 0 {
		params.Limit = 1
	}
	now := clk.Now()

	if d.supportsSkipLocked() {
		return jobFetchSkipLocked(ctx, q, d, now, params)
	}
	return jobFetchSQLite(ctx, q, d, now, params)
}

func jobFetchSkipLocked(ctx context.Context, q querier, d Dialect, now time.Time, params driver.FetchParams) ([]driver.JobRow, error) {
	// Step 1: select IDs with SKIP LOCKED
	selectSQL := fmt.Sprintf(`
SELECT id FROM goncordia_jobs
WHERE queue = %s
  AND state IN ('available', 'scheduled')
  AND run_at <= %s
ORDER BY priority DESC, run_at
LIMIT %s
FOR UPDATE SKIP LOCKED`,
		d.placeholder(1), d.placeholder(2), d.placeholder(3),
	)
	rows, err := q.QueryContext(ctx, selectSQL, params.Queue, now, params.Limit)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	return claimJobs(ctx, q, d, now, ids, params.WorkerID)
}

// jobFetchSQLite uses a transaction-safe approach for SQLite (no SKIP LOCKED).
// Works because SQLite serializes writes; reads from the same connection see the same view.
func jobFetchSQLite(ctx context.Context, q querier, d Dialect, now time.Time, params driver.FetchParams) ([]driver.JobRow, error) {
	selectSQL := fmt.Sprintf(`
SELECT id FROM goncordia_jobs
WHERE queue = %s
  AND state IN ('available', 'scheduled')
  AND run_at <= %s
ORDER BY priority DESC, run_at
LIMIT %s`,
		d.placeholder(1), d.placeholder(2), d.placeholder(3),
	)
	rows, err := q.QueryContext(ctx, selectSQL, params.Queue, now, params.Limit)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	return claimJobs(ctx, q, d, now, ids, params.WorkerID)
}

func claimJobs(ctx context.Context, q querier, d Dialect, now time.Time, ids []int64, workerID string) ([]driver.JobRow, error) {
	placeholderList := make([]string, len(ids))
	args := make([]any, 0, 3+len(ids))
	args = append(args, "running", now, workerID)
	for i, id := range ids {
		placeholderList[i] = d.placeholder(i + 4)
		args = append(args, id)
	}
	updateSQL := fmt.Sprintf(`
UPDATE goncordia_jobs
SET state = %s, attempted_at = %s, attempt_num = attempt_num + 1, worker_id = %s
WHERE id IN (%s)`,
		d.placeholder(1), d.placeholder(2), d.placeholder(3),
		strings.Join(placeholderList, ", "),
	)
	if _, err := q.ExecContext(ctx, updateSQL, args...); err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}

	// Fetch the claimed rows
	selectArgs := make([]any, len(ids))
	idPlaceholders := make([]string, len(ids))
	for i, id := range ids {
		selectArgs[i] = id
		idPlaceholders[i] = d.placeholder(i + 1)
	}
	selectSQL := fmt.Sprintf(`
SELECT id, queue, kind, args, state, priority, run_at, created_at,
       attempted_at, finalized_at, attempt_num, max_retry, timeout_ms,
       unique_key, worker_id, tags, errors
FROM goncordia_jobs WHERE id IN (%s)`,
		strings.Join(idPlaceholders, ", "),
	)
	rows, err := q.QueryContext(ctx, selectSQL, selectArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobRows(d, rows)
}

func jobSetStateIfRunning(ctx context.Context, q querier, d Dialect, clk clock.Clock, params driver.JobSetStateParams) error {
	var finalizedAt *time.Time
	switch params.State {
	case driver.JobStateCompleted, driver.JobStateDiscarded, driver.JobStateCancelled:
		t := clk.Now()
		finalizedAt = &t
	}

	targetState := string(params.State)
	var retryAt *time.Time
	if params.State == driver.JobStateRetryable && !params.RetryAt.IsZero() {
		targetState = string(driver.JobStateAvailable)
		retryAt = &params.RetryAt
	}

	var entryJSON []byte // single AttemptError encoded as JSON object
	if params.Err != nil {
		entry := driver.AttemptError{At: clk.Now(), Error: *params.Err}
		entryJSON, _ = json.Marshal(entry)
	}

	switch d {
	case SQLite:
		return jobSetStateIfRunningSQLite(ctx, q, d, params.ID, targetState, finalizedAt, retryAt, entryJSON)
	case MySQL:
		return jobSetStateIfRunningMySQL(ctx, q, params.ID, targetState, finalizedAt, retryAt, entryJSON)
	default: // Postgres: JSONB concat via ||, cast string to jsonb
		return jobSetStateIfRunningPostgres(ctx, q, params.ID, targetState, finalizedAt, retryAt, entryJSON)
	}
}

func jobSetStateIfRunningPostgres(ctx context.Context, q querier, id, targetState string, finalizedAt, retryAt *time.Time, entryJSON []byte) error {
	// Build SET clauses dynamically to avoid CASE IS NOT NULL with pointer params
	// (pgx prepared statement type inference can be unreliable for nullable timestamps).
	setClauses := []string{"state = $1"}
	args := []any{targetState}
	n := 2

	if finalizedAt != nil {
		setClauses = append(setClauses, fmt.Sprintf("finalized_at = $%d", n))
		args = append(args, *finalizedAt)
		n++
	}
	if retryAt != nil {
		setClauses = append(setClauses, fmt.Sprintf("run_at = $%d", n))
		args = append(args, *retryAt)
		n++
	}
	if entryJSON != nil {
		setClauses = append(setClauses, fmt.Sprintf("errors = errors || $%d::jsonb", n))
		args = append(args, "["+string(entryJSON)+"]")
		n++
	}
	args = append(args, id)

	_, err := q.ExecContext(ctx, fmt.Sprintf(
		"UPDATE goncordia_jobs SET %s WHERE id = $%d AND state = 'running'",
		strings.Join(setClauses, ", "), n,
	), args...)
	return err
}

func jobSetStateIfRunningMySQL(ctx context.Context, q querier, id, targetState string, finalizedAt, retryAt *time.Time, entryJSON []byte) error {
	if entryJSON != nil {
		// Append the new error entry to the JSON array first.
		if _, err := q.ExecContext(ctx,
			`UPDATE goncordia_jobs SET errors = JSON_ARRAY_APPEND(errors, '$', CAST(? AS JSON)) WHERE id = ? AND state = 'running'`,
			string(entryJSON), id,
		); err != nil {
			return err
		}
	}
	_, err := q.ExecContext(ctx, `
UPDATE goncordia_jobs
SET state        = ?,
    finalized_at = CASE WHEN ? IS NOT NULL THEN ? ELSE finalized_at END,
    run_at       = CASE WHEN ? IS NOT NULL THEN ? ELSE run_at END
WHERE id = ? AND state = 'running'`,
		targetState,
		finalizedAt, finalizedAt,
		retryAt, retryAt,
		id,
	)
	return err
}

func jobSetStateIfRunningSQLite(ctx context.Context, q querier, d Dialect, id, targetState string, finalizedAt, retryAt *time.Time, entryJSON []byte) error {
	if entryJSON != nil {
		if _, err := q.ExecContext(ctx,
			fmt.Sprintf(`UPDATE goncordia_jobs SET errors = json_insert(errors, '$[#]', json(%s)) WHERE id = %s AND state = 'running'`,
				d.placeholder(1), d.placeholder(2)),
			string(entryJSON), id,
		); err != nil {
			return err
		}
	}
	_, err := q.ExecContext(ctx, fmt.Sprintf(`
UPDATE goncordia_jobs
SET state        = %s,
    finalized_at = CASE WHEN %s IS NOT NULL THEN %s ELSE finalized_at END,
    run_at       = CASE WHEN %s IS NOT NULL THEN %s ELSE run_at END
WHERE id = %s AND state = 'running'`,
		d.placeholder(1),
		d.placeholder(2), d.placeholder(3),
		d.placeholder(4), d.placeholder(5),
		d.placeholder(6),
	),
		targetState,
		finalizedAt, finalizedAt,
		retryAt, retryAt,
		id,
	)
	return err
}

func jobCancel(ctx context.Context, q querier, d Dialect, clk clock.Clock, id string) error {
	updateSQL := fmt.Sprintf(`
UPDATE goncordia_jobs SET state = 'cancelled', finalized_at = %s
WHERE id = %s AND state IN ('available', 'scheduled')`,
		d.placeholder(1), d.placeholder(2),
	)
	_, err := q.ExecContext(ctx, updateSQL, clk.Now(), id)
	return err
}

func jobDelete(ctx context.Context, q querier, d Dialect, id string) error {
	_, err := q.ExecContext(ctx, fmt.Sprintf(`DELETE FROM goncordia_jobs WHERE id = %s`, d.placeholder(1)), id)
	return err
}

func jobReschedule(ctx context.Context, q querier, d Dialect, params driver.RescheduleParams) error {
	updateSQL := fmt.Sprintf(`UPDATE goncordia_jobs SET state = 'scheduled', run_at = %s WHERE id = %s`,
		d.placeholder(1), d.placeholder(2),
	)
	_, err := q.ExecContext(ctx, updateSQL, params.RunAt, params.ID)
	return err
}

func queueGet(ctx context.Context, q querier, d Dialect, name string) (*driver.QueueRow, error) {
	query := fmt.Sprintf(`SELECT name, paused, created_at, updated_at FROM goncordia_queues WHERE name = %s`, d.placeholder(1))
	row := q.QueryRowContext(ctx, query, name)
	return scanQueueRow(row)
}

func queueSetPaused(ctx context.Context, q querier, d Dialect, clk clock.Clock, name string, paused bool) error {
	var query string
	switch d {
	case MySQL:
		query = fmt.Sprintf(`
INSERT INTO goncordia_queues (name, paused, created_at, updated_at) VALUES (%s, %s, %s, %s)
ON DUPLICATE KEY UPDATE paused = VALUES(paused), updated_at = VALUES(updated_at)`,
			d.placeholder(1), d.placeholder(2), d.placeholder(3), d.placeholder(4),
		)
	case SQLite:
		query = fmt.Sprintf(`
INSERT INTO goncordia_queues (name, paused, created_at, updated_at) VALUES (%s, %s, %s, %s)
ON CONFLICT(name) DO UPDATE SET paused = excluded.paused, updated_at = excluded.updated_at`,
			d.placeholder(1), d.placeholder(2), d.placeholder(3), d.placeholder(4),
		)
	default: // Postgres
		query = fmt.Sprintf(`
INSERT INTO goncordia_queues (name, paused, created_at, updated_at) VALUES (%s, %s, %s, %s)
ON CONFLICT (name) DO UPDATE SET paused = EXCLUDED.paused, updated_at = EXCLUDED.updated_at`,
			d.placeholder(1), d.placeholder(2), d.placeholder(3), d.placeholder(4),
		)
	}
	now := clk.Now()
	_, err := q.ExecContext(ctx, query, name, paused, now, now)
	return err
}

func queueList(ctx context.Context, q querier, d Dialect, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	query := fmt.Sprintf(`SELECT name, paused, created_at, updated_at FROM goncordia_queues ORDER BY name LIMIT %s`, d.placeholder(1))
	rows, err := q.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*driver.QueueRow
	for rows.Next() {
		r, err := scanQueueRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func leaderAttemptElect(ctx context.Context, q querier, d Dialect, params driver.LeaderElectParams) (bool, error) {
	if d == Postgres {
		var elected bool
		err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT pg_try_advisory_lock(hashtext(%s))`, d.placeholder(1)), params.Name).Scan(&elected)
		return elected, err
	}
	return true, nil // single-process for MySQL/SQLite
}

func leaderResign(ctx context.Context, q querier, d Dialect, name string) error {
	if d == Postgres {
		_, err := q.ExecContext(ctx, fmt.Sprintf(`SELECT pg_advisory_unlock(hashtext(%s))`, d.placeholder(1)), name)
		return err
	}
	return nil
}

// --- scan helpers ---

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJobRow(d Dialect, s rowScanner) (*driver.JobRow, error) {
	var (
		r           driver.JobRow
		idStr       string
		state       string
		timeoutMS   int64
		uniqueKey   sql.NullString
		workerID    sql.NullString
		tagsRaw     []byte
		errorsRaw   []byte
	)
	err := s.Scan(
		&idStr, &r.Queue, &r.Kind, &r.Args, &state, &r.Priority, &r.RunAt,
		&r.CreatedAt, &r.AttemptedAt, &r.FinalizedAt, &r.AttemptNum,
		&r.MaxRetry, &timeoutMS, &uniqueKey, &workerID, &tagsRaw, &errorsRaw,
	)
	if err != nil {
		return nil, err
	}
	r.ID = idStr
	r.State = driver.JobState(state)
	r.Timeout = time.Duration(timeoutMS) * time.Millisecond
	if uniqueKey.Valid {
		r.UniqueKey = uniqueKey.String
	}
	if workerID.Valid {
		r.WorkerID = workerID.String
	}
	if len(tagsRaw) > 0 {
		_ = json.Unmarshal(tagsRaw, &r.Tags)
	}
	if len(errorsRaw) > 0 {
		_ = json.Unmarshal(errorsRaw, &r.Errors)
	}
	return &r, nil
}

func scanJobRows(d Dialect, rows *sql.Rows) ([]driver.JobRow, error) {
	var result []driver.JobRow
	for rows.Next() {
		r, err := scanJobRow(d, rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func scanQueueRow(s rowScanner) (*driver.QueueRow, error) {
	var r driver.QueueRow
	var paused any
	if err := s.Scan(&r.Name, &paused, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, err
	}
	// Normalize paused: bool (Postgres) or int (SQLite/MySQL)
	switch v := paused.(type) {
	case bool:
		r.Paused = v
	case int64:
		r.Paused = v != 0
	case []byte:
		r.Paused = len(v) > 0 && v[0] == 1
	}
	return &r, nil
}
