package goncordia

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goncordia/goncordia/core"
	"github.com/goncordia/goncordia/driver"
	"github.com/goncordia/goncordia/internal/clock"
)

// WorkerConfig configures the worker pool.
type WorkerConfig struct {
	// Queues lists the queues this worker pool processes.
	// If empty, only "default" is polled.
	Queues []string
	// Concurrency is the maximum number of jobs running simultaneously.
	// Default: 10.
	Concurrency int
	// PollInterval is how long to wait between polls when the queue is empty.
	// Only used when the backend has no push notification support.
	// Default: 1 second.
	PollInterval time.Duration
	// RetryPolicy controls retry timing. Default: ExponentialRetry.
	RetryPolicy core.RetryPolicy
	// ShutdownTimeout is the max duration to wait for in-flight jobs during shutdown.
	// Default: 30 seconds.
	ShutdownTimeout time.Duration
	// Clock overrides the time source. Defaults to clock.Real{}.
	// Inject clock.NewMock() in tests to control time.
	Clock clock.Clock
}

// WorkerPool processes jobs from the queue using a pool of goroutines.
// TTx is the driver's transaction type (needed only for type parameter inference;
// the pool itself does not open user-visible transactions).
type WorkerPool[TTx any] struct {
	driver   driver.Driver[TTx]
	registry *core.Registry
	config   WorkerConfig

	wg             sync.WaitGroup
	sem            chan struct{}
	shutdownOnce   sync.Once
	shutdownCh     chan struct{}
	isShuttingDown atomic.Bool
}

// NewWorkerPool creates a WorkerPool.
// Register workers using core.RegisterWorker before calling Start.
func NewWorkerPool[TTx any](d driver.Driver[TTx], registry *core.Registry, cfg WorkerConfig) *WorkerPool[TTx] {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.RetryPolicy == nil {
		cfg.RetryPolicy = core.DefaultRetryPolicy
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"default"}
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}

	return &WorkerPool[TTx]{
		driver:     d,
		registry:   registry,
		config:     cfg,
		sem:        make(chan struct{}, cfg.Concurrency),
		shutdownCh: make(chan struct{}),
	}
}

// Start launches the fetch-and-process loops. Blocks until ctx is cancelled or Stop is called.
func (p *WorkerPool[TTx]) Start(ctx context.Context) error {
	if listener := p.driver.Listener(); listener != nil {
		return p.runWithNotifications(ctx, listener)
	}
	return p.runWithPolling(ctx)
}

// Stop initiates a graceful shutdown, waiting up to ShutdownTimeout for in-flight jobs.
func (p *WorkerPool[TTx]) Stop() {
	p.shutdownOnce.Do(func() {
		p.isShuttingDown.Store(true)
		close(p.shutdownCh)
	})

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-p.config.Clock.After(p.config.ShutdownTimeout):
	}
}

// runWithPolling polls the store at PollInterval when no push notifications are available.
func (p *WorkerPool[TTx]) runWithPolling(ctx context.Context) error {
	ticker := p.config.Clock.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.Stop()
			return ctx.Err()
		case <-p.shutdownCh:
			return nil
		case <-ticker.C:
			p.fetchAndDispatch(ctx)
		}
	}
}

// runWithNotifications listens for push notifications and fetches immediately on receipt.
// A fallback ticker also polls periodically in case notifications are missed (e.g. queue resume).
func (p *WorkerPool[TTx]) runWithNotifications(ctx context.Context, l driver.Listener) error {
	fallbackTicker := p.config.Clock.NewTicker(p.config.PollInterval)
	defer fallbackTicker.Stop()

	for _, q := range p.config.Queues {
		ch, err := l.Listen(ctx, q)
		if err != nil {
			return err
		}
		p.wg.Add(1)
		go func(notifications <-chan driver.Notification) {
			defer p.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-p.shutdownCh:
					return
				case _, ok := <-notifications:
					if !ok {
						return
					}
					p.fetchAndDispatch(ctx)
				}
			}
		}(ch)
	}

	for {
		select {
		case <-ctx.Done():
			p.Stop()
			return ctx.Err()
		case <-p.shutdownCh:
			return nil
		case <-fallbackTicker.C:
			p.fetchAndDispatch(ctx)
		}
	}
}

// fetchAndDispatch claims a batch of jobs and starts a goroutine per job.
func (p *WorkerPool[TTx]) fetchAndDispatch(ctx context.Context) {
	if p.isShuttingDown.Load() {
		return
	}

	free := cap(p.sem) - len(p.sem)
	if free <= 0 {
		return
	}

	exec := p.driver.Executor()

	for _, queue := range p.config.Queues {
		rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{
			Queue: queue,
			Limit: free,
		})
		if err != nil || len(rows) == 0 {
			continue
		}

		for i := range rows {
			row := rows[i]
			p.sem <- struct{}{}
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer func() { <-p.sem }()
				p.processRow(ctx, exec, row)
			}()
		}
	}
}

// processRow executes a single job with panic recovery, then updates its state.
func (p *WorkerPool[TTx]) processRow(ctx context.Context, exec driver.Executor, row driver.JobRow) {
	// Resolve effective MaxRetry: job-level overrides worker default; 0 means "use worker default".
	maxRetry := row.MaxRetry
	if maxRetry <= 0 {
		if opts, ok := p.registry.Opts(row.Kind); ok && opts.MaxRetry > 0 {
			maxRetry = opts.MaxRetry
		}
	}
	if maxRetry <= 0 {
		maxRetry = 1 // bare minimum: at least one attempt
	}

	raw := &core.RawJob{
		ID:         row.ID,
		Queue:      row.Queue,
		Kind:       row.Kind,
		Args:       row.Args,
		AttemptNum: row.AttemptNum,
		MaxRetry:   maxRetry,
		Tags:       row.Tags,
	}

	var jobErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				switch v := r.(type) {
				case error:
					jobErr = v
				default:
					jobErr = &panicError{val: v}
				}
			}
		}()
		jobErr = p.registry.Process(ctx, raw)
	}()

	if jobErr == nil {
		_ = exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
			ID:    row.ID,
			State: driver.JobStateCompleted,
		})
		return
	}

	errStr := jobErr.Error()

	if row.AttemptNum >= maxRetry {
		_ = exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
			ID:    row.ID,
			State: driver.JobStateDiscarded,
			Err:   &errStr,
		})
		return
	}

	retryAt := p.config.RetryPolicy.NextRetryAt(row.AttemptNum, jobErr, p.config.Clock)
	_ = exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
		ID:      row.ID,
		State:   driver.JobStateRetryable,
		Err:     &errStr,
		RetryAt: retryAt,
	})
}

type panicError struct{ val any }

func (e *panicError) Error() string {
	return "panic: " + anyToString(e.val)
}

func anyToString(v any) string {
	if s, ok := v.(interface{ Error() string }); ok {
		return s.Error()
	}
	if s, ok := v.(string); ok {
		return s
	}
	return "unknown panic value"
}
