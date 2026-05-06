package core

import (
	"math"
	"time"
)

// RetryPolicy determines how long to wait before a failed job is retried.
type RetryPolicy interface {
	NextRetryAt(attempt int, err error) time.Time
}

// ExponentialRetry implements exponential backoff: delay = base * 2^(attempt-1) + jitter.
// This is the default retry policy.
type ExponentialRetry struct {
	// Base is the initial delay. Default: 1 second.
	Base time.Duration
	// Max caps the calculated delay. Default: 24 hours.
	Max time.Duration
}

// DefaultRetryPolicy is the out-of-the-box exponential backoff.
var DefaultRetryPolicy RetryPolicy = ExponentialRetry{
	Base: time.Second,
	Max:  24 * time.Hour,
}

func (r ExponentialRetry) NextRetryAt(attempt int, _ error) time.Time {
	base := r.Base
	if base == 0 {
		base = time.Second
	}
	max := r.Max
	if max == 0 {
		max = 24 * time.Hour
	}

	delay := time.Duration(float64(base) * math.Pow(2, float64(attempt-1)))
	if delay > max {
		delay = max
	}
	return time.Now().Add(delay)
}

// FixedRetry retries after a constant delay.
type FixedRetry struct {
	Delay time.Duration
}

func (r FixedRetry) NextRetryAt(_ int, _ error) time.Time {
	return time.Now().Add(r.Delay)
}

// NoRetry discards a job after the first failure.
type NoRetry struct{}

// NextRetryAt returns a zero time, indicating no retry.
func (NoRetry) NextRetryAt(_ int, _ error) time.Time { return time.Time{} }
