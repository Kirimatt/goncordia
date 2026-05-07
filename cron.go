package goncordia

import (
	"context"
	"time"

	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/internal/clock"
)

// PeriodicJob pairs a Schedule with the job args to enqueue on each tick.
type PeriodicJob struct {
	// Schedule determines when the job runs.
	Schedule core.Schedule
	// Args is the job to enqueue.
	Args core.JobArgs
	// Opts are passed through to Enqueue on each tick (optional).
	Opts *core.InsertOpts
}

// CronConfig configures a CronScheduler.
type CronConfig struct {
	// TickInterval controls how often the scheduler checks for due jobs.
	// Default: 1 second.
	TickInterval time.Duration
	// Clock overrides the time source. Defaults to clock.Real{}.
	Clock clock.Clock
}

// CronScheduler enqueues periodic jobs on a configurable tick.
// It does not process jobs — pair it with a WorkerPool.
//
// Usage:
//
//	cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
//	    {Schedule: core.Every(time.Hour), Args: CleanupArgs{}},
//	}, goncordia.CronConfig{})
//	go cs.Start(ctx)
type CronScheduler[TTx any] struct {
	client  *Client[TTx]
	entries []cronEntry
	config  CronConfig
}

type cronEntry struct {
	job     PeriodicJob
	lastRun time.Time
}

// NewCronScheduler creates a CronScheduler backed by d.
// jobs is the list of periodic jobs to manage.
func NewCronScheduler[TTx any](d driver.Driver[TTx], jobs []PeriodicJob, cfg CronConfig) *CronScheduler[TTx] {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = time.Second
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}

	entries := make([]cronEntry, len(jobs))
	for i, j := range jobs {
		entries[i] = cronEntry{job: j}
	}

	return &CronScheduler[TTx]{
		client:  NewClient[TTx](d, ClientConfig{}),
		entries: entries,
		config:  cfg,
	}
}

// Start begins the scheduling loop. It blocks until ctx is cancelled.
func (s *CronScheduler[TTx]) Start(ctx context.Context) error {
	ticker := s.config.Clock.NewTicker(s.config.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *CronScheduler[TTx]) tick(ctx context.Context) {
	now := s.config.Clock.Now()
	for i := range s.entries {
		e := &s.entries[i]
		next := e.job.Schedule.Next(e.lastRun)
		if next.IsZero() || !now.Before(next) {
			_, _ = s.client.Enqueue(ctx, e.job.Args, e.job.Opts)
			e.lastRun = now
		}
	}
}
