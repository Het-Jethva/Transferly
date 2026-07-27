package terminal

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Het-Jethva/Transferly/internal/session"
)

func (a *App) progress(label string, total int64) session.Progress {
	lastReported := int64(-1)
	return func(completed int64) {
		if completed != 0 && completed != total && completed-lastReported < 1024*1024 {
			return
		}
		lastReported = completed
		a.line("%s: %d/%d bytes", label, completed, total)
	}
}

func (a *App) reportSessionError(remote string, err error) {
	var versionError *session.VersionError
	switch {
	case errors.Is(err, session.ErrLocalRejected):
		a.line("Verification did not match; connection closed.")
	case errors.Is(err, session.ErrPeerRejected):
		a.line("Peer rejected verification; connection closed.")
	case errors.Is(err, session.ErrPeerBusy):
		a.line("%v.", err)
	case errors.As(err, &versionError):
		a.line("Incompatible wire protocol: local %s, Peer %s. Install compatible Transferly releases.", versionError.Local, versionError.Peer)
	case errors.Is(err, context.Canceled):
		// disconnect and quit already explain the action.
	default:
		a.line("Could not establish a secure Transfer Session with %s: %v. Check that both Peers run compatible Transferly releases.", remote, err)
	}
}

func (a *App) finishFailed(current *attempt) {
	a.mu.Lock()
	becameAvailable := a.current == current && !a.closed
	if a.current == current {
		a.current = nil
	}
	current.waiting = false
	a.mu.Unlock()
	current.cancel()
	if becameAvailable {
		a.setDiscoveryAvailable(true)
	}
}

func (a *App) disconnect() {
	a.mu.Lock()
	current := a.current
	if current == nil {
		a.mu.Unlock()
		a.line("No active Transfer Session.")
		return
	}
	current.stopping = true
	secured := current.secure
	raw := current.raw
	a.mu.Unlock()

	a.line("Disconnecting from %s...", current.remote)
	current.cancel()
	if secured != nil {
		_ = secured.Close()
	} else if raw != nil {
		_ = raw.Close()
	}
}

func (a *App) shutdown() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	current := a.current
	a.current = nil
	var secured *session.Session
	var raw net.Conn
	if current != nil {
		current.stopping = true
		secured = current.secure
		raw = current.raw
	}
	a.mu.Unlock()

	a.cancelRoot()
	_ = a.listener.Close()
	if current != nil {
		current.cancel()
		if secured != nil {
			_ = secured.Close()
		} else if raw != nil {
			_ = raw.Close()
		}
		select {
		case <-current.finished:
		case <-time.After(2 * time.Second):
			a.line("Cleanup is still stopping; destination-local incomplete data may require removal.")
		}
	}
	a.line("Transferly stopped. No identity or trust was saved.")
}

func (a *App) line(format string, arguments ...any) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	_, _ = fmt.Fprintf(a.output, format+"\n", arguments...)
}
