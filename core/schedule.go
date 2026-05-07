package core

import "time"

// Schedule determines when a periodic job should next run.
type Schedule interface {
	// Next returns the next run time given the time of the last run.
	// If last is zero (first run), implementations should return the zero
	// time to signal "run immediately on the first tick".
	Next(last time.Time) time.Time
}

// ScheduleFunc adapts a plain function to the Schedule interface.
type ScheduleFunc func(last time.Time) time.Time

func (f ScheduleFunc) Next(last time.Time) time.Time { return f(last) }

type everySchedule struct{ d time.Duration }

// Every returns a Schedule that fires every d duration.
// The job runs on the first scheduler tick, then every d thereafter.
func Every(d time.Duration) Schedule { return everySchedule{d: d} }

func (s everySchedule) Next(last time.Time) time.Time {
	if last.IsZero() {
		return time.Time{} // zero → fire immediately
	}
	return last.Add(s.d)
}
