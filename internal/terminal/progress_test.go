package terminal

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestOfferProgressRateLimitsAggregateReports(t *testing.T) {
	now := time.Unix(1, 0)
	clock := &progressTestClock{now: &now}
	output := &bytes.Buffer{}
	application := &App{clock: clock, output: output}
	progress := newOfferProgress(application, 1, 3*1024*1024)

	observe, finished := progress.begin("large.bin", 3*1024*1024)
	observe(1024 * 1024)
	now = now.Add(time.Second - time.Millisecond)
	observe(2 * 1024 * 1024)
	if reports := strings.Count(output.String(), "Active files:"); reports != 1 {
		t.Fatalf("aggregate reports before interval = %d, want 1\n%s", reports, output.String())
	}

	now = now.Add(time.Millisecond)
	observe(3 * 1024 * 1024)
	if reports := strings.Count(output.String(), "Active files:"); reports != 2 {
		t.Fatalf("aggregate reports after interval = %d, want 2\n%s", reports, output.String())
	}
	if strings.Contains(output.String(), "Sending large.bin:") {
		t.Fatalf("legacy per-file progress was printed:\n%s", output.String())
	}

	finished()
	progress.complete("large.bin", true)
	if !strings.Contains(output.String(), "Item succeeded: large.bin") {
		t.Fatalf("per-file outcome was not printed:\n%s", output.String())
	}
}

type progressTestClock struct {
	now *time.Time
}

func (c *progressTestClock) Now() time.Time {
	return *c.now
}

func (*progressTestClock) NewTimerAt(time.Time) sessionTimer {
	panic("progress test does not create timers")
}
