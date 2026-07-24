package terminal

import (
	"time"

	"github.com/Het-Jethva/Transferly/internal/session"
)

func (a *App) advanceControllableTime(line string) bool {
	if !a.config.ControllableTime {
		return false
	}
	command, argument := splitCommand(line)
	if command != "advance-time" {
		return false
	}
	duration, err := time.ParseDuration(argument)
	if err != nil || duration <= 0 {
		a.line("Usage: advance-time <positive-duration>")
		return true
	}
	manual, ok := a.clock.(*manualSessionClock)
	if !ok {
		return false
	}
	manual.Advance(duration)
	return true
}

func (a *App) keepAlive() {
	a.mu.Lock()
	current := a.current
	if current == nil || current.secure == nil {
		a.mu.Unlock()
		a.line("Open and verify a Transfer Session before using keep-alive.")
		return
	}
	secured := current.secure
	a.mu.Unlock()
	a.line("Transfer Session kept alive.")
	go func() {
		if err := secured.Send(session.Message{Type: messageKeepAlive}); err != nil && current.context.Err() == nil {
			a.line("Could not keep the Transfer Session alive: %v", err)
		}
	}()
}

func (a *App) noteTerminalActivity() {
	a.mu.Lock()
	current := a.current
	var secured *session.Session
	if current != nil && current.secure != nil {
		a.noteSessionActivityLocked(current, a.clock.Now())
		if !current.transferActive {
			secured = current.secure
		}
	}
	a.mu.Unlock()
	if secured != nil {
		_ = secured.Send(session.Message{Type: messageActivity})
	}
}

func (a *App) noteSessionActivity(current *attempt) {
	a.mu.Lock()
	if a.current == current && current.secure != nil {
		a.noteSessionActivityLocked(current, a.clock.Now())
	}
	a.mu.Unlock()
}

func (a *App) noteSessionActivityLocked(current *attempt, now time.Time) {
	current.lastActivity = now
	current.idleWarned = false
	select {
	case current.idleWake <- struct{}{}:
	default:
	}
}

func (a *App) startTransfer(current *attempt) {
	a.mu.Lock()
	if a.current == current && current.secure != nil {
		current.transferActive = true
		select {
		case current.idleWake <- struct{}{}:
		default:
		}
	}
	a.mu.Unlock()
}

func (a *App) finishTransferLocked(current *attempt) {
	current.transferActive = false
	a.noteSessionActivityLocked(current, a.clock.Now())
}

func (a *App) monitorIdleSession(current *attempt) {
	for {
		a.mu.Lock()
		if a.current != current || current.secure == nil || current.stopping {
			a.mu.Unlock()
			return
		}
		if current.transferActive {
			wake := current.idleWake
			a.mu.Unlock()
			select {
			case <-wake:
			case <-current.context.Done():
				return
			}
			continue
		}

		elapsed := a.clock.Now().Sub(current.lastActivity)
		if elapsed >= a.config.IdleTimeoutAfter {
			current.stopping = true
			secured := current.secure
			a.mu.Unlock()
			a.line("Transfer Session expired after %s without activity; temporary verification was discarded.", a.config.IdleTimeoutAfter)
			current.cancel()
			_ = secured.Close()
			return
		}
		if elapsed >= a.config.IdleWarningAfter && !current.idleWarned {
			current.idleWarned = true
			a.mu.Unlock()
			a.line("Transfer Session idle for %s; it will disconnect after %s. Type keep-alive to continue.", a.config.IdleWarningAfter, a.config.IdleTimeoutAfter)
			continue
		}
		deadline := current.lastActivity.Add(a.config.IdleWarningAfter)
		if current.idleWarned {
			deadline = current.lastActivity.Add(a.config.IdleTimeoutAfter)
		}
		wake := current.idleWake
		a.mu.Unlock()

		timer := a.clock.NewTimerAt(deadline)
		select {
		case <-timer.Channel():
		case <-wake:
			stopSessionTimer(timer)
		case <-current.context.Done():
			stopSessionTimer(timer)
			return
		}
	}
}
