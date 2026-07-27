package terminal

import (
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

func stopSessionTimer(timer sessionTimer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.Channel():
	default:
	}
}
