package terminal

import (
	"context"
	"errors"

	"github.com/Het-Jethva/Transferly/internal/session"
)

func (a *App) cancelActiveOffer() bool {
	a.mu.Lock()
	current := a.current
	if current == nil || current.secure == nil || !current.transferActive {
		a.mu.Unlock()
		return false
	}
	var offerID string
	if current.outgoing != nil {
		if current.outgoing.canceled {
			a.mu.Unlock()
			return true
		}
		current.outgoing.canceled = true
		offerID = current.outgoing.id
	} else if current.incoming != nil {
		if current.incoming.canceled {
			a.mu.Unlock()
			return true
		}
		current.incoming.canceled = true
		offerID = current.incoming.id
	} else {
		a.mu.Unlock()
		return false
	}
	secured := current.secure
	a.mu.Unlock()
	a.line("Canceling the active Transfer Offer...")
	go func() {
		if err := secured.Send(session.Message{Type: "cancel", OfferID: offerID}); err != nil && current.context.Err() == nil {
			a.line("Could not request Transfer Offer cancellation: %v", err)
		}
	}()
	return true
}

func (a *App) handlePeerCancellation(current *attempt, message session.Message) error {
	a.mu.Lock()
	if current.outgoing != nil && current.outgoing.id == message.OfferID {
		current.outgoing.canceled = true
		a.mu.Unlock()
		// Echo the ordered cancellation after stopping future chunks so the
		// receiving Peer can clean every open staging stream and acknowledge it.
		return current.secure.Send(session.Message{Type: "cancel", OfferID: message.OfferID})
	}
	incoming := current.incoming
	if incoming == nil || incoming.id != message.OfferID || !incoming.accepted {
		a.mu.Unlock()
		return nil
	}
	incoming.canceled = true
	a.mu.Unlock()

	a.removeIncomingFiles(incoming)
	success := false
	if err := current.secure.Send(session.Message{Type: "result", OfferID: incoming.id, Success: &success, Reason: reasonOfferCanceled}); err != nil {
		return err
	}
	a.clearIncoming(current, incoming)
	a.line("Transfer Offer canceled; incomplete content was removed and completed files were retained.")
	return nil
}

func (a *App) abortIncoming(current *attempt, message session.Message) {
	a.mu.Lock()
	incoming := current.incoming
	a.mu.Unlock()
	if incoming == nil || incoming.id != message.OfferID {
		return
	}
	a.removeIncomingFiles(incoming)
	a.clearIncoming(current, incoming)
	reason := message.Reason
	if reason == "" {
		reason = "the sender stopped the transfer"
	}
	a.line("Transfer failed for %s: %s; incomplete content was removed.", message.Path, reason)
}

func (a *App) clearIncoming(current *attempt, incoming *incomingOffer) {
	a.mu.Lock()
	if current.incoming != incoming {
		a.mu.Unlock()
		return
	}
	a.finishTransferLocked(current)
	current.incoming = nil
	if current.role == session.Inbound {
		current.coordinatorActive = false
		next := a.takeCoordinatorOfferLocked(current)
		a.mu.Unlock()
		a.startCoordinatorOffer(current, next)
		return
	}
	next := a.takeNextOutgoingLocked(current)
	a.mu.Unlock()
	if next != nil {
		go a.runOutgoing(current, next)
	}
}

func (a *App) takeCoordinatorOfferLocked(current *attempt) *queuedOffer {
	if current.role != session.Inbound || current.coordinatorActive || len(current.coordinatorQueue) == 0 {
		return nil
	}
	next := current.coordinatorQueue[0]
	current.coordinatorQueue = current.coordinatorQueue[1:]
	current.coordinatorActive = true
	if next.incoming != nil {
		current.incoming = next.incoming
	} else {
		current.outgoing = next.outgoing
	}
	return &next
}

func (a *App) startCoordinatorOffer(current *attempt, next *queuedOffer) {
	if next == nil {
		return
	}
	if next.incoming != nil {
		a.startIncomingReview(current, next.incoming)
		return
	}
	go a.runOutgoing(current, next.outgoing)
}

func (a *App) startIncomingReview(current *attempt, incoming *incomingOffer) {
	a.mu.Lock()
	if current.incoming != incoming {
		a.mu.Unlock()
		return
	}
	done := make(chan struct{})
	incoming.reviewDone = done
	a.mu.Unlock()
	go func() {
		defer close(done)
		if err := a.reviewIncomingOffer(current, incoming); err != nil && !errors.Is(err, context.Canceled) {
			_ = current.secure.Close()
		}
	}()
}
