package redisdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/internal/clock"
)

// ---- key schema ----

const (
	jobKeyPrefix = "goncordia:job:"
	queuesSetKey = "goncordia:queues"
)

func availKey(q string) string       { return "goncordia:q:" + q + ":avail" }
func schedKey(q string) string       { return "goncordia:q:" + q + ":sched" }
func runKey(q string) string         { return "goncordia:q:" + q + ":run" }
func metaKey(q string) string        { return "goncordia:q:" + q + ":meta" }
func jobKey(id string) string        { return jobKeyPrefix + id }
func uniqKey(q, k string) string     { return "goncordia:uniq:" + q + ":" + k }
func leaderKey(n string) string      { return "goncordia:leader:" + n }
func notifyChannel(q string) string  { return "goncordia:notify:" + q }

// priorityScore encodes priority and run_at into a sorted-set score.
// ZPOPMIN picks lowest score, so higher priority → lower score → claimed first.
func priorityScore(priority int, runAt time.Time) float64 {
	return float64(runAt.UnixMilli()) - float64(priority)*1e12
}

// ---- job document ----

type redisJob struct {
	ID            string           `json:"id"`
	Queue         string           `json:"queue"`
	Kind          string           `json:"kind"`
	Args          string           `json:"args"` // raw JSON string
	State         string           `json:"state"`
	Priority      int              `json:"priority"`
	RunAtMs       int64            `json:"run_at_ms"`
	CreatedAtMs   int64            `json:"created_at_ms"`
	AttemptedAtMs int64            `json:"attempted_at_ms,omitempty"`
	FinalizedAtMs int64            `json:"finalized_at_ms,omitempty"`
	AttemptNum    int              `json:"attempt_num"`
	MaxRetry      int              `json:"max_retry"`
	TimeoutMs     int64            `json:"timeout_ms"`
	UniqueKey     string           `json:"unique_key,omitempty"`
	WorkerID      string           `json:"worker_id,omitempty"`
	Tags          []string         `json:"tags"`
	Errors        []redisAttemptErr `json:"errors"`
}

type redisAttemptErr struct {
	AtMs    int64  `json:"at_ms"`
	Attempt int    `json:"attempt"`
	Message string `json:"message"`
}

func jobToRow(j redisJob) *driver.JobRow {
	row := &driver.JobRow{
		ID:         j.ID,
		Queue:      j.Queue,
		Kind:       j.Kind,
		Args:       []byte(j.Args),
		State:      driver.JobState(j.State),
		Priority:   j.Priority,
		RunAt:      time.UnixMilli(j.RunAtMs).UTC(),
		CreatedAt:  time.UnixMilli(j.CreatedAtMs).UTC(),
		AttemptNum: j.AttemptNum,
		MaxRetry:   j.MaxRetry,
		Timeout:    time.Duration(j.TimeoutMs) * time.Millisecond,
		UniqueKey:  j.UniqueKey,
		WorkerID:   j.WorkerID,
		Tags:       j.Tags,
	}
	if j.AttemptedAtMs != 0 {
		t := time.UnixMilli(j.AttemptedAtMs).UTC()
		row.AttemptedAt = &t
	}
	if j.FinalizedAtMs != 0 {
		t := time.UnixMilli(j.FinalizedAtMs).UTC()
		row.FinalizedAt = &t
	}
	for _, e := range j.Errors {
		row.Errors = append(row.Errors, driver.AttemptError{
			At:      time.UnixMilli(e.AtMs).UTC(),
			Attempt: e.Attempt,
			Error:   e.Message,
		})
	}
	return row
}

// ---- Lua: atomic fetch-and-claim ----
//
// KEYS[1] = avail sorted set
// KEYS[2] = sched sorted set
// KEYS[3] = running hash
// ARGV[1] = now_ms
// ARGV[2] = worker_id  (unused by script; Go sets it after fetch)
// ARGV[3] = job key prefix
//
// Returns: job ID string, or empty string when queue is empty.
var fetchOneScript = redis.NewScript(`
local avail  = KEYS[1]
local sched  = KEYS[2]
local run_h  = KEYS[3]
local now_ms = tonumber(ARGV[1])
local prefix = ARGV[3]

-- Promote due scheduled jobs to the available set.
local due = redis.call('ZRANGEBYSCORE', sched, '-inf', tostring(now_ms))
for i = 1, #due do
    local id  = due[i]
    local raw = redis.call('GET', prefix .. id)
    if raw and raw ~= false then
        local ok, job = pcall(cjson.decode, raw)
        local priority  = 0
        local run_at_ms = now_ms
        if ok and type(job) == 'table' then
            priority  = tonumber(job.priority)   or 0
            run_at_ms = tonumber(job.run_at_ms)  or now_ms
        end
        redis.call('ZREM',  sched, id)
        redis.call('ZADD',  avail, run_at_ms - priority * 1000000000000, id)
    end
end

-- Claim one job.
local res = redis.call('ZPOPMIN', avail, 1)
if #res == 0 then return '' end

local id = res[1]
redis.call('HSET', run_h, id, tostring(now_ms))
return id
`)

// ---- executor ----

type executor struct {
	rdb *redis.Client
	clk clock.Clock
}

func (e *executor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return &txExecutor{executor: *e}, nil
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.rdb, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.rdb, id)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.rdb, e.clk, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.rdb, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.rdb, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.rdb, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.rdb, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.rdb, e.clk, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.rdb, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.rdb, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.rdb, e.clk, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.rdb, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, e.rdb, name)
}

// ---- txExecutor ----

type txExecutor struct{ executor }

func (t *txExecutor) Commit(_ context.Context) error   { return nil }
func (t *txExecutor) Rollback(_ context.Context) error { return nil }
func (t *txExecutor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}

func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, t.rdb, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, t.rdb, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, t.rdb, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, t.rdb, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, t.rdb, t.clk, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.rdb, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.rdb, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, t.rdb, name)
}

// ---- core functions ----

func jobInsertMany(ctx context.Context, rdb *redis.Client, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
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

		// Unique-key deduplication: SET NX
		if p.UniqueKey != "" {
			ok, err := rdb.SetNX(ctx, uniqKey(p.Queue, p.UniqueKey), id, 0).Result()
			if err != nil {
				return nil, fmt.Errorf("unique key check: %w", err)
			}
			if !ok {
				results[i] = driver.JobInsertResult{UniqueSkip: true}
				continue
			}
		}

		tags := p.Tags
		if tags == nil {
			tags = []string{}
		}
		job := redisJob{
			ID:          id,
			Queue:       p.Queue,
			Kind:        p.Kind,
			Args:        string(p.Args),
			State:       string(state),
			Priority:    p.Priority,
			RunAtMs:     runAt.UnixMilli(),
			CreatedAtMs: now.UnixMilli(),
			MaxRetry:    p.MaxRetry,
			TimeoutMs:   p.Timeout.Milliseconds(),
			UniqueKey:   p.UniqueKey,
			Tags:        tags,
			Errors:      []redisAttemptErr{},
		}

		raw, err := json.Marshal(job)
		if err != nil {
			return nil, fmt.Errorf("marshal job: %w", err)
		}

		pipe := rdb.Pipeline()
		pipe.Set(ctx, jobKey(id), raw, 0)
		if state == driver.JobStateScheduled {
			pipe.ZAdd(ctx, schedKey(p.Queue), redis.Z{Score: float64(runAt.UnixMilli()), Member: id})
		} else {
			pipe.ZAdd(ctx, availKey(p.Queue), redis.Z{Score: priorityScore(p.Priority, runAt), Member: id})
		}
		ensureQueueMeta(pipe, ctx, p.Queue, now)
		pipe.Publish(ctx, notifyChannel(p.Queue), "1")
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("insert job: %w", err)
		}

		results[i] = driver.JobInsertResult{Job: jobToRow(job)}
	}
	return results, nil
}

func jobGetByID(ctx context.Context, rdb *redis.Client, id string) (*driver.JobRow, error) {
	raw, err := rdb.Get(ctx, jobKey(id)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, err
	}
	return jobToRow(job), nil
}

func jobFetchBatch(ctx context.Context, rdb *redis.Client, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	// Check pause state.
	paused, err := isQueuePaused(ctx, rdb, params.Queue)
	if err != nil {
		return nil, err
	}
	if paused {
		return nil, nil
	}

	now := clk.Now()
	nowMs := now.UnixMilli()

	var rows []driver.JobRow
	for range params.Limit {
		id, err := fetchOneScript.Run(ctx, rdb,
			[]string{availKey(params.Queue), schedKey(params.Queue), runKey(params.Queue)},
			nowMs, params.WorkerID, jobKeyPrefix,
		).Text()
		if err != nil || id == "" {
			break
		}

		raw, err := rdb.Get(ctx, jobKey(id)).Bytes()
		if err != nil {
			// Job key missing (unlikely); remove from running hash and skip.
			rdb.HDel(ctx, runKey(params.Queue), id) //nolint:errcheck
			continue
		}

		var job redisJob
		if err := json.Unmarshal(raw, &job); err != nil {
			continue
		}

		t := nowMs
		job.State = string(driver.JobStateRunning)
		job.AttemptedAtMs = t
		job.AttemptNum++
		job.WorkerID = params.WorkerID

		updated, _ := json.Marshal(job)
		rdb.Set(ctx, jobKey(id), updated, 0) //nolint:errcheck

		rows = append(rows, *jobToRow(job))
	}
	return rows, nil
}

func jobSetStateIfRunning(ctx context.Context, rdb *redis.Client, clk clock.Clock, params driver.JobSetStateParams) error {
	raw, err := rdb.Get(ctx, jobKey(params.ID)).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}

	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return err
	}
	if job.State != string(driver.JobStateRunning) {
		return nil
	}

	now := clk.Now()

	// Remove from running hash regardless of transition.
	rdb.HDel(ctx, runKey(job.Queue), params.ID) //nolint:errcheck

	if params.Err != nil {
		job.Errors = append(job.Errors, redisAttemptErr{
			AtMs:    now.UnixMilli(),
			Attempt: job.AttemptNum,
			Message: *params.Err,
		})
	}

	if params.State == driver.JobStateRetryable {
		retryAt := params.RetryAt
		if retryAt.IsZero() {
			retryAt = now
		}
		job.State = string(driver.JobStateAvailable)
		job.RunAtMs = retryAt.UnixMilli()

		updated, _ := json.Marshal(job)
		pipe := rdb.Pipeline()
		pipe.Set(ctx, jobKey(params.ID), updated, 0)
		if retryAt.After(now) {
			pipe.ZAdd(ctx, schedKey(job.Queue), redis.Z{Score: float64(retryAt.UnixMilli()), Member: params.ID})
		} else {
			pipe.ZAdd(ctx, availKey(job.Queue), redis.Z{Score: priorityScore(job.Priority, retryAt), Member: params.ID})
		}
		_, err = pipe.Exec(ctx)
		return err
	}

	// Terminal state.
	job.State = string(params.State)
	job.FinalizedAtMs = now.UnixMilli()
	if job.UniqueKey != "" {
		rdb.Del(ctx, uniqKey(job.Queue, job.UniqueKey)) //nolint:errcheck
	}
	updated, _ := json.Marshal(job)
	return rdb.Set(ctx, jobKey(params.ID), updated, 0).Err()
}

func jobCancel(ctx context.Context, rdb *redis.Client, clk clock.Clock, id string) error {
	raw, err := rdb.Get(ctx, jobKey(id)).Bytes()
	if err == redis.Nil {
		return fmt.Errorf("job %q not found", id)
	}
	if err != nil {
		return err
	}
	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return err
	}
	if job.State != string(driver.JobStateAvailable) && job.State != string(driver.JobStateScheduled) {
		return fmt.Errorf("job %q is in state %s, can only cancel available/scheduled", id, job.State)
	}

	now := clk.Now()
	job.State = string(driver.JobStateCancelled)
	job.FinalizedAtMs = now.UnixMilli()
	if job.UniqueKey != "" {
		rdb.Del(ctx, uniqKey(job.Queue, job.UniqueKey)) //nolint:errcheck
	}

	pipe := rdb.Pipeline()
	pipe.ZRem(ctx, availKey(job.Queue), id)
	pipe.ZRem(ctx, schedKey(job.Queue), id)
	updated, _ := json.Marshal(job)
	pipe.Set(ctx, jobKey(id), updated, 0)
	_, err = pipe.Exec(ctx)
	return err
}

func jobDelete(ctx context.Context, rdb *redis.Client, id string) error {
	raw, err := rdb.Get(ctx, jobKey(id)).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil
	}
	pipe := rdb.Pipeline()
	pipe.Del(ctx, jobKey(id))
	pipe.ZRem(ctx, availKey(job.Queue), id)
	pipe.ZRem(ctx, schedKey(job.Queue), id)
	pipe.HDel(ctx, runKey(job.Queue), id)
	if job.UniqueKey != "" {
		pipe.Del(ctx, uniqKey(job.Queue, job.UniqueKey))
	}
	_, err = pipe.Exec(ctx)
	return err
}

func jobReschedule(ctx context.Context, rdb *redis.Client, params driver.RescheduleParams) error {
	raw, err := rdb.Get(ctx, jobKey(params.ID)).Bytes()
	if err == redis.Nil {
		return fmt.Errorf("job %q not found", params.ID)
	}
	if err != nil {
		return err
	}
	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return err
	}
	job.RunAtMs = params.RunAt.UnixMilli()
	job.State = string(driver.JobStateScheduled)

	pipe := rdb.Pipeline()
	pipe.ZRem(ctx, availKey(job.Queue), params.ID)
	pipe.ZAdd(ctx, schedKey(job.Queue), redis.Z{Score: float64(params.RunAt.UnixMilli()), Member: params.ID})
	updated, _ := json.Marshal(job)
	pipe.Set(ctx, jobKey(params.ID), updated, 0)
	_, err = pipe.Exec(ctx)
	return err
}

// ---- queue metadata ----

func ensureQueueMeta(pipe redis.Pipeliner, ctx context.Context, name string, now time.Time) {
	nowMs := now.UnixMilli()
	pipe.SAdd(ctx, queuesSetKey, name)
	// HSetNX: only sets if field doesn't already exist.
	pipe.HSetNX(ctx, metaKey(name), "paused", "0")
	pipe.HSetNX(ctx, metaKey(name), "created_at_ms", fmt.Sprintf("%d", nowMs))
	pipe.HSetNX(ctx, metaKey(name), "updated_at_ms", fmt.Sprintf("%d", nowMs))
}

func isQueuePaused(ctx context.Context, rdb *redis.Client, name string) (bool, error) {
	val, err := rdb.HGet(ctx, metaKey(name), "paused").Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val == "1", nil
}

func queueGet(ctx context.Context, rdb *redis.Client, clk clock.Clock, name string) (*driver.QueueRow, error) {
	vals, err := rdb.HGetAll(ctx, metaKey(name)).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	return parseQueueRow(name, vals), nil
}

func queueSetPaused(ctx context.Context, rdb *redis.Client, clk clock.Clock, name string, paused bool) error {
	now := clk.Now()
	val := "0"
	if paused {
		val = "1"
	}
	pipe := rdb.Pipeline()
	pipe.SAdd(ctx, queuesSetKey, name)
	pipe.HSetNX(ctx, metaKey(name), "created_at_ms", fmt.Sprintf("%d", now.UnixMilli()))
	pipe.HSet(ctx, metaKey(name), "paused", val, "updated_at_ms", fmt.Sprintf("%d", now.UnixMilli()))
	_, err := pipe.Exec(ctx)
	return err
}

func queueList(ctx context.Context, rdb *redis.Client, clk clock.Clock, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	names, err := rdb.SMembers(ctx, queuesSetKey).Result()
	if err != nil {
		return nil, err
	}
	rows := make([]*driver.QueueRow, 0, len(names))
	for _, name := range names {
		vals, err := rdb.HGetAll(ctx, metaKey(name)).Result()
		if err != nil || len(vals) == 0 {
			continue
		}
		rows = append(rows, parseQueueRow(name, vals))
	}
	return rows, nil
}

func parseQueueRow(name string, vals map[string]string) *driver.QueueRow {
	row := &driver.QueueRow{Name: name}
	row.Paused = vals["paused"] == "1"
	if ms := parseInt64(vals["created_at_ms"]); ms != 0 {
		row.CreatedAt = time.UnixMilli(ms).UTC()
	}
	if ms := parseInt64(vals["updated_at_ms"]); ms != 0 {
		row.UpdatedAt = time.UnixMilli(ms).UTC()
	}
	return row
}

func parseInt64(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}

// ---- leader election ----

func leaderAttemptElect(ctx context.Context, rdb *redis.Client, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	key := leaderKey(params.Name)

	// Try to become leader (NX = only set if not exists).
	ok, err := rdb.SetNX(ctx, key, params.WorkerID, params.TTL).Result()
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}

	// Key exists; check if it belongs to us (renew TTL).
	current, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current == params.WorkerID {
		rdb.Expire(ctx, key, params.TTL) //nolint:errcheck
		return true, nil
	}
	return false, nil
}

func leaderResign(ctx context.Context, rdb *redis.Client, name string) error {
	return rdb.Del(ctx, leaderKey(name)).Err()
}

// ---- listener ----

type listener struct {
	rdb  *redis.Client
	mu   sync.Mutex
	subs map[string]*redisSub
}

type redisSub struct {
	ps *redis.PubSub
	ch chan driver.Notification
}

func (l *listener) Listen(ctx context.Context, queue string) (<-chan driver.Notification, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.subs == nil {
		l.subs = make(map[string]*redisSub)
	}
	if _, ok := l.subs[queue]; ok {
		return l.subs[queue].ch, nil
	}

	ch := make(chan driver.Notification, 16)
	ps := l.rdb.Subscribe(ctx, notifyChannel(queue))
	l.subs[queue] = &redisSub{ps: ps, ch: ch}

	go func() {
		defer close(ch)
		for range ps.Channel() {
			select {
			case ch <- driver.Notification{Queue: queue}:
			default:
			}
		}
	}()

	return ch, nil
}

func (l *listener) Unlisten(_ context.Context, queue string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if sub, ok := l.subs[queue]; ok {
		sub.ps.Close() //nolint:errcheck
		delete(l.subs, queue)
	}
	return nil
}

func (l *listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, sub := range l.subs {
		sub.ps.Close() //nolint:errcheck
	}
	l.subs = make(map[string]*redisSub)
	return nil
}

// compile-time checks
var _ driver.Executor = (*executor)(nil)
var _ driver.ExecutorTx = (*txExecutor)(nil)
