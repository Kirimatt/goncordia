package clock_test

import (
	"testing"
	"time"

	"github.com/kirimatt/goncordia/internal/clock"
)

func TestMockClock(t *testing.T) {
	base := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	clk := clock.NewMock(base)

	if got := clk.Now(); !got.Equal(base) {
		t.Fatalf("expected %v, got %v", base, got)
	}

	clk.Advance(5 * time.Hour)
	if got := clk.Now(); !got.Equal(base.Add(5 * time.Hour)) {
		t.Fatalf("expected %v, got %v", base.Add(5*time.Hour), got)
	}

	clk.Set(base)
	if got := clk.Now(); !got.Equal(base) {
		t.Fatalf("after Set: expected %v, got %v", base, got)
	}
}

func TestMockSince(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewMock(base)

	past := base.Add(-3 * time.Hour)
	if got := clk.Since(past); got != 3*time.Hour {
		t.Fatalf("expected 3h, got %v", got)
	}
}
