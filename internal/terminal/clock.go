package terminal

import (
	"sync"
	"time"
)

type sessionClock interface {
	Now() time.Time
	NewTimerAt(time.Time) sessionTimer
}

type sessionTimer interface {
	Channel() <-chan time.Time
	Stop() bool
}

type realSessionClock struct{}

func (realSessionClock) Now() time.Time {
	return time.Now()
}

func (realSessionClock) NewTimerAt(deadline time.Time) sessionTimer {
	return &realSessionTimer{timer: time.NewTimer(time.Until(deadline))}
}

type realSessionTimer struct {
	timer *time.Timer
}

func (t *realSessionTimer) Channel() <-chan time.Time {
	return t.timer.C
}

func (t *realSessionTimer) Stop() bool {
	return t.timer.Stop()
}

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

func stopSessionTimer(timer sessionTimer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.Channel():
	default:
	}
}
