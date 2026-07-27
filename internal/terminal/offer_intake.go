package terminal

import (
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/Het-Jethva/Transferly/internal/session"
)

func (a *App) handleIncomingOffer(current *attempt, message session.Message) error {
	entryCount := int64(message.FileCount) + int64(message.FolderCount)
	if len(message.OfferID) != 32 || message.RootCount < 1 || message.RootCount > maxManifestEntries || message.FileCount < 0 || message.FolderCount < 0 || entryCount < 1 || entryCount > maxManifestEntries || message.TotalBytes < 0 {
		return errors.New("Peer sent an invalid or oversized Transfer Offer header")
	}
	if _, err := hex.DecodeString(message.OfferID); err != nil {
		return errors.New("Peer sent an invalid Transfer Offer identifier")
	}
	incoming := &incomingOffer{id: message.OfferID, destination: a.config.DefaultDestination, actions: make(chan offerAction, 1), collecting: true, rootCount: message.RootCount}
	incoming.manifest.FileCount, incoming.manifest.FolderCount, incoming.manifest.TotalBytes = message.FileCount, message.FolderCount, message.TotalBytes
	a.mu.Lock()
	if current.pendingIncoming == nil {
		current.pendingIncoming = make(map[string]*incomingOffer)
	}
	if _, exists := current.pendingIncoming[incoming.id]; exists {
		a.mu.Unlock()
		return errors.New("Peer repeated a Transfer Offer identifier")
	}
	current.pendingIncoming[incoming.id] = incoming
	a.mu.Unlock()
	return nil
}

func (a *App) handleIncomingManifest(current *attempt, message session.Message) error {
	a.mu.Lock()
	incoming := current.pendingIncoming[message.OfferID]
	a.mu.Unlock()
	if incoming == nil || !incoming.collecting {
		return errors.New("Peer sent manifest data without a matching Transfer Offer")
	}
	switch message.Type {
	case "manifest-entry":
		if len(incoming.manifest.Entries) >= maxManifestEntries {
			return errors.New("Peer sent too many manifest entries")
		}
		if err := validateManifestPath(message.Path); err != nil {
			return fmt.Errorf("Peer sent unsafe manifest path %q: %w", message.Path, err)
		}
		if (message.Kind != manifestFile && message.Kind != manifestFolder) || message.Size < 0 || (message.Kind == manifestFolder && message.Size != 0) {
			return errors.New("Peer sent an invalid manifest entry")
		}
		if (message.Kind == manifestFile && (len(message.Digest) != 64 || !isHexDigest(message.Digest))) || (message.Kind == manifestFolder && message.Digest != "") {
			return errors.New("Peer sent an invalid manifest digest")
		}
		incoming.manifest.Entries = append(incoming.manifest.Entries, manifestEntry{Path: message.Path, Kind: message.Kind, Size: message.Size, Modified: time.Unix(0, message.Modified), ReadOnly: message.ReadOnly, Hidden: message.Hidden, Digest: strings.ToLower(message.Digest)})
	case "manifest-omission":
		if len(incoming.manifest.Omissions) >= maxManifestOmissions {
			return errors.New("Peer sent too many manifest omissions")
		}
		if err := validateManifestPath(message.Path); err != nil {
			return fmt.Errorf("Peer sent unsafe omission path %q: %w", message.Path, err)
		}
		if message.Reason == "" || len(message.Reason) > maxOmissionReason {
			return errors.New("Peer sent an invalid manifest omission reason")
		}
		incoming.manifest.Omissions = append(incoming.manifest.Omissions, manifestOmission{Path: message.Path, Reason: message.Reason})
	case "offer-complete":
		if err := validateReceivedManifest(incoming); err != nil {
			return err
		}
		incoming.collecting = false
		a.mu.Lock()
		delete(current.pendingIncoming, incoming.id)
		if current.role == session.Inbound {
			if len(current.coordinatorQueue) >= maxQueuedOffers {
				a.mu.Unlock()
				accepted := false
				return current.secure.Send(session.Message{Type: "decision", OfferID: incoming.id, Accepted: &accepted, Reason: "Transfer Offer queue is full"})
			}
			current.coordinatorQueue = append(current.coordinatorQueue, queuedOffer{incoming: incoming})
			next := a.takeCoordinatorOfferLocked(current)
			a.mu.Unlock()
			if err := current.secure.Send(session.Message{Type: "queued", OfferID: incoming.id}); err != nil {
				return err
			}
			a.startCoordinatorOffer(current, next)
			return nil
		}
		if current.incoming != nil {
			a.mu.Unlock()
			return errors.New("coordinating Peer sent overlapping Transfer Offers")
		}
		current.incoming = incoming
		a.mu.Unlock()
		a.startIncomingReview(current, incoming)
	}
	return nil
}

func validateReceivedManifest(incoming *incomingOffer) error {
	files, folders := 0, 0
	var bytes int64
	seen := make(map[string]manifestEntry, len(incoming.manifest.Entries))
	incoming.manifest.Roots = nil
	for _, entry := range incoming.manifest.Entries {
		if err := validateManifestPath(entry.Path); err != nil {
			return fmt.Errorf("Peer sent unsafe manifest path %q: %w", entry.Path, err)
		}
		key := manifestPathKey(entry.Path)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("Peer sent manifest paths with a case-insensitive or Unicode alias: %q", entry.Path)
		}
		seen[key] = entry
		switch entry.Kind {
		case manifestFile:
			files++
			if entry.Size > incoming.manifest.TotalBytes-bytes {
				return errors.New("Peer sent a Transfer Offer whose byte total overflows or is inconsistent")
			}
			bytes += entry.Size
		case manifestFolder:
			folders++
		default:
			return errors.New("Peer sent an unsupported manifest entry kind")
		}
		if !strings.Contains(entry.Path, "/") {
			incoming.manifest.Roots = append(incoming.manifest.Roots, entry.Path)
		}
	}
	for _, entry := range incoming.manifest.Entries {
		parent := entry.Path
		for strings.Contains(parent, "/") {
			parent = parent[:strings.LastIndex(parent, "/")]
			parentEntry, exists := seen[manifestPathKey(parent)]
			if !exists {
				return fmt.Errorf("Peer sent manifest path %q without its parent folder %q", entry.Path, parent)
			}
			if parentEntry.Kind != manifestFolder {
				return fmt.Errorf("Peer sent an invalid manifest hierarchy: file is used as a parent for %q", entry.Path)
			}
		}
	}
	if files != incoming.manifest.FileCount || folders != incoming.manifest.FolderCount || bytes != incoming.manifest.TotalBytes || len(incoming.manifest.Roots) != incoming.rootCount {
		return errors.New("Peer sent an inconsistent Transfer Offer manifest")
	}
	return nil
}

func (a *App) reviewIncomingOffer(current *attempt, incoming *incomingOffer) error {
	if err := resolveIncomingPaths(incoming); err != nil {
		return err
	}
	a.mu.Lock()
	incoming.waiting = true
	a.mu.Unlock()
	a.showIncoming(incoming)
	for {
		select {
		case action := <-incoming.actions:
			switch action.kind {
			case "details":
				a.showManifestDetails(incoming)
			case "cleanup-staging":
				if !incoming.staleStaging {
					a.line("There is no stale Transferly staging data at this destination.")
					continue
				}
				if err := cleanupStaleStaging(incoming.destination); err != nil {
					a.line("Could not remove stale Transferly staging data: %v", err)
					continue
				}
				incoming.staleStaging = false
				a.line("Stale Transferly staging data removed. It was not used as resume state.")
			case "destination":
				destination, err := filepath.Abs(action.destination)
				if err != nil {
					a.line("Cannot use destination %q: %v", action.destination, err)
					continue
				}
				incoming.destination = filepath.Clean(destination)
				if err := resolveIncomingPaths(incoming); err != nil {
					a.line("Cannot use destination %q: %v", action.destination, err)
					continue
				}
				a.line("Destination updated for this Transfer Offer only.")
				a.showIncoming(incoming)
			case "reject":
				a.mu.Lock()
				incoming.waiting = false
				a.mu.Unlock()
				accepted := false
				if err := current.secure.Send(session.Message{Type: "decision", OfferID: incoming.id, Accepted: &accepted}); err != nil {
					return err
				}
				a.clearIncoming(current, incoming)
				a.line("Transfer Offer rejected. No file content was written.")
				return nil
			case "accept":
				if err := prepareIncoming(incoming); err != nil {
					a.line("Cannot accept Transfer Offer at %s: %v", incoming.destination, err)
					continue
				}
				a.mu.Lock()
				incoming.waiting = false
				incoming.accepted = true
				a.mu.Unlock()
				a.startTransfer(current)
				accepted := true
				if err := current.secure.Send(session.Message{Type: "decision", OfferID: incoming.id, Accepted: &accepted}); err != nil {
					a.removeIncomingFiles(incoming)
					a.clearIncoming(current, incoming)
					return err
				}
				a.line("Transfer Offer accepted. Waiting for file content.")
				return nil
			}
		case <-current.context.Done():
			return current.context.Err()
		}
	}
}

func resolveIncomingPaths(incoming *incomingOffer) error {
	destination, reserved, err := destinationNameReservations(incoming.destination)
	if err != nil {
		return err
	}
	incoming.destination = destination
	incoming.staleStaging, err = hasStaleStaging(destination)
	if err != nil {
		return fmt.Errorf("inspect Transferly staging data: %w", err)
	}
	incoming.finalPaths = make(map[string]string)
	for _, root := range incoming.manifest.Roots {
		path, err := resolveFinalPathWithReservations(destination, root, reserved)
		if err != nil {
			return fmt.Errorf("resolve top-level destination for %q: %w", root, err)
		}
		incoming.finalPaths[manifestPathKey(root)] = path
	}
	return nil
}

func (a *App) showIncoming(incoming *incomingOffer) {
	manifest := incoming.manifest
	if manifest.FileCount == 1 && manifest.FolderCount == 0 && len(manifest.Roots) == 1 {
		a.line("Transfer Offer: %s (%d bytes)", manifest.Roots[0], manifest.TotalBytes)
	} else {
		a.line("Transfer Offer: %d top-level roots, %d files, %d folders (%d bytes)", len(manifest.Roots), manifest.FileCount, manifest.FolderCount, manifest.TotalBytes)
		a.line("Top-level roots: %s", strings.Join(manifest.Roots, ", "))
	}
	a.line("Destination: %s", incoming.destination)
	for _, root := range manifest.Roots {
		a.line("Final path: %s", incoming.finalPaths[manifestPathKey(root)])
	}
	if executableCount := len(executablePaths(manifest)); executableCount > 0 {
		a.line("WARNING: %d executable or script file(s) in this Transfer Offer. Review details before accepting.", executableCount)
	}
	if len(manifest.Omissions) > 0 {
		a.line("Omissions: %d unsupported, unreadable, or vanished entries (type details to review)", len(manifest.Omissions))
	}
	if incoming.staleStaging {
		a.line("Stale Transferly staging data detected at this destination. It is not resumable; type cleanup-staging to remove it safely before accepting.")
	}
	if manifest.FileCount == 1 && manifest.FolderCount == 0 {
		a.line("Choose accept, reject, or destination <path>.")
	} else {
		a.line("Choose accept, reject, destination <path>, or details.")
	}
}

func (a *App) showManifestDetails(incoming *incomingOffer) {
	a.line("Complete manifest:")
	for _, entry := range incoming.manifest.Entries {
		attributes := make([]string, 0, 2)
		if entry.Hidden {
			attributes = append(attributes, "hidden")
		}
		if entry.ReadOnly {
			attributes = append(attributes, "read-only")
		}
		if len(attributes) == 0 {
			attributes = append(attributes, "none")
		}
		if entry.Kind == manifestFile {
			warning := ""
			if isExecutableOrScript(entry.Path) {
				warning = " [EXECUTABLE OR SCRIPT]"
			}
			a.line("  file %s%s (%d bytes), modified %s, attributes %s", entry.Path, warning, entry.Size, entry.Modified.Format(time.RFC3339Nano), strings.Join(attributes, ", "))
		} else {
			a.line("  folder %s, modified %s, attributes %s", entry.Path, entry.Modified.Format(time.RFC3339Nano), strings.Join(attributes, ", "))
		}
	}
	for _, omission := range incoming.manifest.Omissions {
		a.line("  omitted %s: %s", omission.Path, omission.Reason)
	}
}
