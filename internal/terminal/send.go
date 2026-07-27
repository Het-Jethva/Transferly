package terminal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/Het-Jethva/Transferly/internal/session"
)

func (a *App) sendPaths(paths []string) {
	a.mu.Lock()
	current := a.current
	if current == nil || current.secure == nil {
		a.mu.Unlock()
		a.line("Open and verify a Transfer Session before using send.")
		return
	}
	a.mu.Unlock()

	manifest, err := buildManifest(paths)
	if err != nil {
		a.line("Cannot create Transfer Offer: %v", err)
		return
	}
	id, err := newOfferID()
	if err != nil {
		a.line("Cannot create Transfer Offer: %v", err)
		return
	}
	outgoing := &outgoingOffer{id: id, manifest: manifest, decision: make(chan session.Message, 1), result: make(chan session.Message, 8), queued: make(chan bool, 1)}

	a.mu.Lock()
	if a.current != current || current.secure == nil {
		a.mu.Unlock()
		a.line("The Transfer Session changed before the offer could be sent; try again.")
		return
	}
	label := outgoingLabel(outgoing)
	if current.role == session.Inbound {
		if len(current.coordinatorQueue) >= maxQueuedOffers {
			a.mu.Unlock()
			a.line("Transfer Offer queue is full; wait for an offer to finish before sending %s.", label)
			return
		}
		queued := current.coordinatorActive || len(current.coordinatorQueue) > 0
		current.coordinatorQueue = append(current.coordinatorQueue, queuedOffer{outgoing: outgoing})
		next := a.takeCoordinatorOfferLocked(current)
		a.mu.Unlock()
		if queued {
			a.line("Transfer Offer queued: %s (%d bytes).", label, manifest.TotalBytes)
		}
		a.startCoordinatorOffer(current, next)
		return
	}
	if current.outgoing != nil {
		if len(current.outgoingQueue) >= maxQueuedOffers {
			a.mu.Unlock()
			a.line("Transfer Offer queue is full; wait for an offer to finish before sending %s.", label)
			return
		}
		outgoing.submitted = true
		current.outgoingQueue = append(current.outgoingQueue, outgoing)
		secured := current.secure
		a.mu.Unlock()
		if err := sendManifest(secured, outgoing); err != nil {
			a.line("Could not queue Transfer Offer %s: %v", label, err)
			_ = secured.Close()
			return
		}
		select {
		case queued := <-outgoing.queued:
			if queued {
				a.line("Transfer Offer queued: %s (%d bytes).", label, manifest.TotalBytes)
			} else {
				a.line("Peer could not queue Transfer Offer %s.", label)
			}
		case <-current.context.Done():
		}
		return
	}
	outgoing.submitted = true
	current.outgoing = outgoing
	secured := current.secure
	a.mu.Unlock()
	if err := sendManifest(secured, outgoing); err != nil {
		a.failOutgoing(current, outgoing, "Could not send Transfer Offer: %v", err)
		_ = secured.Close()
		return
	}
	go a.runOutgoing(current, outgoing)
}

func outgoingLabel(outgoing *outgoingOffer) string {
	if len(outgoing.manifest.Roots) == 1 {
		return outgoing.manifest.Roots[0]
	}
	return fmt.Sprintf("%d roots", len(outgoing.manifest.Roots))
}

func sendManifest(secured *session.Session, outgoing *outgoingOffer) error {
	manifest := outgoing.manifest
	if err := secured.Send(session.Message{Type: "offer", OfferID: outgoing.id, RootCount: len(manifest.Roots), FileCount: manifest.FileCount, FolderCount: manifest.FolderCount, TotalBytes: manifest.TotalBytes}); err != nil {
		return err
	}
	for _, entry := range manifest.Entries {
		if err := secured.Send(session.Message{Type: "manifest-entry", OfferID: outgoing.id, Path: entry.Path, Kind: entry.Kind, Size: entry.Size, Modified: entry.Modified.UnixNano(), ReadOnly: entry.ReadOnly, Hidden: entry.Hidden, Digest: entry.Digest}); err != nil {
			return err
		}
	}
	for _, omission := range manifest.Omissions {
		if err := secured.Send(session.Message{Type: "manifest-omission", OfferID: outgoing.id, Path: omission.Path, Reason: omission.Reason}); err != nil {
			return err
		}
	}
	return secured.Send(session.Message{Type: "offer-complete", OfferID: outgoing.id})
}

func (a *App) runOutgoing(current *attempt, outgoing *outgoingOffer) {
	if !outgoing.submitted {
		if err := sendManifest(current.secure, outgoing); err != nil {
			a.failOutgoing(current, outgoing, "Could not send Transfer Offer: %v", err)
			return
		}
		outgoing.submitted = true
	}
	manifest := outgoing.manifest
	a.line("Transfer Offer sent: %s (%d bytes). Waiting for the Peer.", outgoingLabel(outgoing), manifest.TotalBytes)
	var decision session.Message
	select {
	case decision = <-outgoing.decision:
	case <-current.context.Done():
		return
	}
	if decision.Accepted == nil {
		a.failOutgoing(current, outgoing, "Peer sent an invalid Transfer Offer decision.")
		return
	}
	if !*decision.Accepted {
		a.clearOutgoing(current, outgoing)
		a.line("Peer rejected Transfer Offer %s. No file content was sent.", outgoingLabel(outgoing))
		return
	}

	a.startTransfer(current)
	files := make([]*manifestEntry, 0, manifest.FileCount)
	for index := range manifest.Entries {
		if manifest.Entries[index].Kind == manifestFile {
			files = append(files, &manifest.Entries[index])
		}
	}
	concurrency := adaptiveFileConcurrency(files)
	if concurrency > 1 {
		a.line("Adaptive scheduling: up to %d concurrent file streams with bounded buffers.", concurrency)
	}
	progress := newOfferProgress(a, manifest.FileCount, manifest.TotalBytes)
	jobs := make(chan *manifestEntry, len(files))
	for _, entry := range files {
		jobs <- entry
	}
	close(jobs)

	workerErrors := make(chan error, concurrency)
	var workers sync.WaitGroup
	var started sync.WaitGroup
	started.Add(concurrency)
	release := make(chan struct{})
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			first := true
			for entry := range jobs {
				observe, streamDone := progress.begin(entry.Path, entry.Size)
				if first {
					started.Done()
					<-release
					first = false
				}
				a.mu.Lock()
				canceled := outgoing.canceled
				a.mu.Unlock()
				if canceled {
					streamDone()
					return
				}
				err := a.sendManifestFile(current, outgoing, entry, observe)
				streamDone()
				if errors.Is(err, errOfferCanceled) {
					return
				}
				if err != nil {
					if sendError := current.secure.Send(session.Message{Type: "file-failed", OfferID: outgoing.id, Path: entry.Path, Reason: err.Error()}); sendError != nil {
						select {
						case workerErrors <- fmt.Errorf("transfer %s: %w", entry.Path, sendError):
						default:
						}
						return
					}
				}
			}
		}()
	}
	started.Wait()
	close(release)

	succeeded, failed := 0, 0
	for succeeded+failed < manifest.FileCount {
		var result session.Message
		select {
		case err := <-workerErrors:
			a.failOutgoing(current, outgoing, "Transfer failed: %v", err)
			_ = current.secure.Close()
			return
		case result = <-outgoing.result:
		case <-current.context.Done():
			return
		}
		if result.Success == nil || !*result.Success {
			if result.Reason == reasonOfferCanceled {
				a.clearOutgoing(current, outgoing)
				a.line("Transfer Offer canceled; incomplete content was removed and completed files were retained.")
				return
			}
			failed++
			progress.complete(result.Path, false)
			a.line("Transfer failed for %s: %s.", result.Path, result.Reason)
			continue
		}
		succeeded++
		progress.complete(result.Path, true)
	}
	workers.Wait()
	if err := current.secure.Send(session.Message{Type: "batch-complete", OfferID: outgoing.id}); err != nil {
		a.failOutgoing(current, outgoing, "Could not complete Transfer Offer: %v", err)
		return
	}
	var result session.Message
	select {
	case result = <-outgoing.result:
	case <-current.context.Done():
		return
	}
	a.clearOutgoing(current, outgoing)
	if failed > 0 {
		a.line("Transfer Offer partially completed: %d of %d files succeeded; %d failed.", succeeded, manifest.FileCount, failed)
		return
	}
	if result.Success == nil || !*result.Success {
		a.line("Transfer Offer failed: %s.", result.Reason)
		return
	}
	if manifest.FileCount == 1 && manifest.FolderCount == 0 {
		a.line("Transfer complete: %s (%d bytes).", manifest.Roots[0], manifest.TotalBytes)
	} else {
		a.line("Transfer complete: %d files, %d folders (%d bytes).", manifest.FileCount, manifest.FolderCount, manifest.TotalBytes)
	}
}

func adaptiveFileConcurrency(files []*manifestEntry) int {
	// Four or more files provide enough independent filesystem work to offset
	// scheduling overhead. Smaller batches stay serial for lower latency.
	if len(files) == 0 {
		return 0
	}
	if len(files) < 4 {
		return 1
	}
	return 4
}

func (a *App) sendManifestFile(current *attempt, outgoing *outgoingOffer, entry *manifestEntry, progress session.Progress) error {
	source, err := os.Open(entry.SourcePath)
	if err != nil {
		return fmt.Errorf("source could not be opened: %w", err)
	}
	defer source.Close()
	before, err := source.Stat()
	if err != nil || before.Size() != entry.Size || !before.ModTime().Equal(entry.Modified) {
		return errors.New("source changed after approval")
	}
	hasher := sha256.New()
	buffer := make([]byte, streamChunkBytes)
	progress(0)
	for offset := int64(0); offset < entry.Size || entry.Size == 0 && offset == 0; {
		a.mu.Lock()
		canceled := outgoing.canceled
		a.mu.Unlock()
		if canceled {
			return errOfferCanceled
		}
		chunkBytes := int64(len(buffer))
		if remaining := entry.Size - offset; remaining < chunkBytes {
			chunkBytes = remaining
		}
		if chunkBytes > 0 {
			sourceReader := a.faultStreamReader(current.context, source)
			if _, err := io.ReadFull(sourceReader, buffer[:chunkBytes]); err != nil {
				return errors.New("source changed or could not be read completely")
			}
			_, _ = hasher.Write(buffer[:chunkBytes])
		}
		if err := current.secure.SendChunkChecked(current.context, session.Message{Type: "content", OfferID: outgoing.id, Path: entry.Path, Size: chunkBytes, Offset: offset}, bytes.NewReader(buffer[:chunkBytes]), chunkBytes, func() error {
			a.mu.Lock()
			defer a.mu.Unlock()
			if outgoing.canceled {
				return errOfferCanceled
			}
			return nil
		}); err != nil {
			if errors.Is(err, errOfferCanceled) {
				return err
			}
			return errors.New("source changed or could not be sent completely")
		}
		offset += chunkBytes
		progress(offset)
		if entry.Size == 0 {
			break
		}
	}

	digest := hex.EncodeToString(hasher.Sum(nil))
	after, statError := source.Stat()
	completion := session.Message{Type: "complete", OfferID: outgoing.id, Path: entry.Path, Size: entry.Size, Digest: digest}
	failed := false
	if statError != nil || after.Size() != entry.Size || !after.ModTime().Equal(entry.Modified) || !strings.EqualFold(digest, entry.Digest) {
		completion.Success = &failed
		completion.Reason = "source changed after approval"
	}
	completion.Digest = a.faultDigest(completion.Digest)
	return current.secure.SendChecked(completion, func() error {
		a.mu.Lock()
		defer a.mu.Unlock()
		if outgoing.canceled {
			return errOfferCanceled
		}
		return nil
	})
}

func (a *App) failOutgoing(current *attempt, outgoing *outgoingOffer, format string, arguments ...any) {
	a.clearOutgoing(current, outgoing)
	a.line(format, arguments...)
}

func (a *App) clearOutgoing(current *attempt, outgoing *outgoingOffer) {
	a.mu.Lock()
	if current.outgoing != outgoing {
		a.mu.Unlock()
		return
	}
	a.finishTransferLocked(current)
	if current.role == session.Inbound {
		current.outgoing = nil
		current.coordinatorActive = false
		next := a.takeCoordinatorOfferLocked(current)
		a.mu.Unlock()
		a.startCoordinatorOffer(current, next)
		return
	}
	current.outgoing = nil
	next := a.takeNextOutgoingLocked(current)
	a.mu.Unlock()
	if next != nil {
		go a.runOutgoing(current, next)
	}
}

func (a *App) takeNextOutgoingLocked(current *attempt) *outgoingOffer {
	if current.incoming != nil || current.outgoing != nil || len(current.outgoingQueue) == 0 {
		return nil
	}
	next := current.outgoingQueue[0]
	current.outgoingQueue = current.outgoingQueue[1:]
	current.outgoing = next
	return next
}
