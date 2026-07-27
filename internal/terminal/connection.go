package terminal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/Het-Jethva/Transferly/internal/session"
)

func (a *App) connect(endpoint string) {
	if err := validatePeerEndpoint(endpoint); err != nil {
		a.line("Invalid endpoint %q: %v", endpoint, err)
		return
	}
	current, ok := a.beginAttempt(endpoint)
	if !ok {
		a.line("Already connected to a Peer or establishing a Transfer Session; disconnect first.")
		return
	}

	a.line("Connecting to %s...", endpoint)
	go func() {
		defer current.finishOnce.Do(func() { close(current.finished) })
		connection, err := a.config.PeerDial(current.context, endpoint)
		if err != nil {
			a.finishFailed(current)
			a.line("Unable to connect to %s: %v. Check the IPv4 address, port, route, and Windows Firewall.", endpoint, err)
			return
		}
		if !a.attachConnection(current, connection) {
			_ = connection.Close()
			return
		}
		a.establish(current, connection, session.Outbound)
	}()
}

func (a *App) acceptConnections() {
	for {
		connection, err := a.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				a.line("Listener error: %v", err)
			}
			return
		}
		remote := connection.RemoteAddr().String()
		current, ok := a.beginAttempt(remote)
		if !ok {
			go func() {
				_ = session.RejectBusy(a.rootContext, connection, a.config.Version)
				_ = connection.Close()
			}()
			continue
		}
		if !a.attachConnection(current, connection) {
			_ = connection.Close()
			continue
		}
		a.line("Incoming connection from %s.", remote)
		go a.establish(current, connection, session.Inbound)
	}
}

func (a *App) beginAttempt(remote string) (*attempt, bool) {
	a.mu.Lock()
	if a.closed || a.current != nil {
		a.mu.Unlock()
		return nil, false
	}
	attemptContext, cancel := context.WithCancel(a.rootContext)
	current := &attempt{
		context:  attemptContext,
		cancel:   cancel,
		remote:   remote,
		answer:   make(chan bool, 1),
		idleWake: make(chan struct{}, 1),
		finished: make(chan struct{}),
	}
	a.current = current
	a.mu.Unlock()
	a.setDiscoveryAvailable(false)
	return current, true
}

func (a *App) attachConnection(current *attempt, connection net.Conn) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != current || a.closed || current.stopping {
		return false
	}
	current.raw = connection
	return true
}

func (a *App) establish(current *attempt, connection net.Conn, role session.Role) {
	defer current.finishOnce.Do(func() { close(current.finished) })
	established := false
	defer func() {
		if !established {
			_ = connection.Close()
		}
	}()

	secured, err := session.Open(current.context, connection, role, a.config.Version, func(ctx context.Context, code string) (bool, error) {
		a.mu.Lock()
		if a.current != current || a.closed {
			a.mu.Unlock()
			return false, context.Canceled
		}
		current.waiting = true
		a.mu.Unlock()

		a.line("Verification code: %s", code)
		a.line("Compare this code with the other Peer. Type yes if it matches, or no if it does not.")
		select {
		case answer := <-current.answer:
			return answer, nil
		case <-ctx.Done():
			a.mu.Lock()
			current.waiting = false
			a.mu.Unlock()
			return false, ctx.Err()
		}
	})
	if err != nil {
		a.finishFailed(current)
		a.reportSessionError(current.remote, err)
		return
	}

	a.mu.Lock()
	if a.current != current || a.closed || current.stopping {
		a.mu.Unlock()
		_ = secured.Close()
		a.finishFailed(current)
		return
	}
	current.secure = secured
	current.role = role
	current.waiting = false
	current.lastActivity = a.clock.Now()
	established = true
	a.mu.Unlock()
	a.line("Transfer Session verified with %s.", current.remote)
	go a.monitorIdleSession(current)

	waitError := a.serveSession(current)
	current.cancel()
	a.cleanupIncoming(current)
	a.mu.Lock()
	wasCurrent := a.current == current
	becameAvailable := wasCurrent && !a.closed
	if wasCurrent {
		a.current = nil
	}
	a.mu.Unlock()
	_ = secured.Close()
	if becameAvailable {
		a.setDiscoveryAvailable(true)
	}
	if wasCurrent {
		if waitError != nil {
			a.line("Transfer Session ended after a connection error: %v", waitError)
		} else {
			a.line("Transfer Session ended.")
		}
	}
}

func (a *App) serveSession(current *attempt) error {
	for {
		message, err := current.secure.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		a.noteSessionActivity(current)
		switch message.Type {
		case "offer":
			if err := a.handleIncomingOffer(current, message); err != nil {
				return err
			}
		case "manifest-entry", "manifest-omission", "offer-complete":
			if err := a.handleIncomingManifest(current, message); err != nil {
				return err
			}
		case "queued":
			if !a.routeQueuedOutgoing(current, message.OfferID) {
				return errors.New("received a queue acknowledgement for an unknown Transfer Offer")
			}
		case "decision":
			if !a.routeOutgoing(current, message, true) {
				return errors.New("received a decision for an unknown Transfer Offer")
			}
		case "content":
			if err := a.receiveContent(current, message); err != nil {
				return err
			}
		case "complete":
			if err := a.completeIncomingFile(current, message); err != nil {
				return err
			}
		case "file-failed":
			if err := a.receiveFileFailure(current, message); err != nil {
				return err
			}
		case "result":
			if !a.routeOutgoing(current, message, false) {
				return errors.New("received a result for an unknown Transfer Offer")
			}
		case "batch-complete":
			if err := a.completeIncomingBatch(current, message); err != nil {
				return err
			}
		case "abort":
			a.abortIncoming(current, message)
		case "cancel":
			if err := a.handlePeerCancellation(current, message); err != nil {
				return err
			}
		case messageActivity:
			if a.timeControlEnabled() {
				a.line("Test clock: Peer terminal activity observed.")
			}
		case messageKeepAlive:
			a.line("Peer kept the Transfer Session alive.")
		default:
			return fmt.Errorf("unexpected frame %q while session is active", message.Type)
		}
	}
}

func (a *App) routeQueuedOutgoing(current *attempt, offerID string) bool {
	a.mu.Lock()
	outgoing := findOutgoingLocked(current, offerID)
	a.mu.Unlock()
	if outgoing == nil {
		return false
	}
	select {
	case outgoing.queued <- true:
		return true
	case <-current.context.Done():
		return false
	}
}

func (a *App) routeOutgoing(current *attempt, message session.Message, decision bool) bool {
	a.mu.Lock()
	outgoing := current.outgoing
	if decision {
		outgoing = findOutgoingLocked(current, message.OfferID)
	}
	if outgoing == nil || outgoing.id != message.OfferID {
		a.mu.Unlock()
		return false
	}
	channel := outgoing.result
	if decision {
		channel = outgoing.decision
	}
	a.mu.Unlock()
	if decision && message.Reason == "Transfer Offer queue is full" {
		select {
		case outgoing.queued <- false:
		default:
		}
	}
	select {
	case channel <- message:
		return true
	case <-current.context.Done():
		return false
	}
}

func findOutgoingLocked(current *attempt, offerID string) *outgoingOffer {
	if current.outgoing != nil && current.outgoing.id == offerID {
		return current.outgoing
	}
	for _, outgoing := range current.outgoingQueue {
		if outgoing.id == offerID {
			return outgoing
		}
	}
	return nil
}
