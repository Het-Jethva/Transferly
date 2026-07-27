//go:build transferly_faults

package terminal

import (
	"sync"
	"time"
)

// manualSessionClock lets a process-level test advance session time on demand
// so idle warning and expiry behavior is asserted deterministically instead of
// by waiting out real minutes.
type manualSessionClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualSessionTimer]struct{}
}

func newManualSessionClock() *manualSessionClock {
	return &manualSessionClock{
		now:    time.Unix(0, 0),
		timers: make(map[*manualSessionTimer]struct{}),
	}
}

func (c *manualSessionClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualSessionClock) NewTimerAt(deadline time.Time) sessionTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &manualSessionTimer{
		clock:    c,
		deadline: deadline,
		channel:  make(chan time.Time, 1),
		active:   true,
	}
	if deadline.After(c.now) {
		c.timers[timer] = struct{}{}
	} else {
		timer.active = false
		timer.channel <- c.now
	}
	return timer
}

func (c *manualSessionClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	for timer := range c.timers {
		if timer.deadline.After(c.now) {
			continue
		}
		delete(c.timers, timer)
		timer.active = false
		timer.channel <- c.now
	}
	c.mu.Unlock()
}

type manualSessionTimer struct {
	clock    *manualSessionClock
	deadline time.Time
	channel  chan time.Time
	active   bool
}

func (t *manualSessionTimer) Channel() <-chan time.Time {
	return t.channel
}

func (t *manualSessionTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if !t.active {
		return false
	}
	delete(t.clock.timers, t)
	t.active = false
	return true
}
