// Package clock provides a Clock interface and implementations for real and mock time.
// Inject Clock everywhere time.Now() is called so tests can control time precisely.
package clock

import (
	"sync"
	"time"
)

// Clock abstracts time so it can be faked in tests.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) *time.Ticker
}

// Real is the production Clock backed by the standard time package.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) Since(t time.Time) time.Duration        { return time.Since(t) }
func (Real) After(d time.Duration) <-chan time.Time  { return time.After(d) }
func (Real) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }

// Mock is a controllable fake clock for tests.
// All methods are safe for concurrent use.
type Mock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewMock creates a Mock clock set to the given time.
// If zero, defaults to 2024-01-01 00:00:00 UTC.
func NewMock(t time.Time) *Mock {
	if t.IsZero() {
		t = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Mock{now: t}
}

// Set sets the mock clock to an absolute time.
func (m *Mock) Set(t time.Time) {
	m.mu.Lock()
	m.now = t
	m.mu.Unlock()
}

// Advance moves the mock clock forward by d.
func (m *Mock) Advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	m.mu.Unlock()
}

func (m *Mock) Now() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.now
}

func (m *Mock) Since(t time.Time) time.Duration {
	return m.Now().Sub(t)
}

// After returns a channel that fires after d relative to the mock clock's current time.
// NOTE: this channel fires based on wall-clock time, not mock time. For tests that need
// to advance time manually and trigger timers, use Advance() + check return values
// from functions that accept a Clock rather than relying on After() channels.
func (m *Mock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// NewTicker returns a real ticker — mock tests should prefer polling mock.Now()
// rather than relying on ticker intervals.
func (m *Mock) NewTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}
