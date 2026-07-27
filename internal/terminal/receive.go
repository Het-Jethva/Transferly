package terminal

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Het-Jethva/Transferly/internal/session"
)

func (a *App) receiveContent(current *attempt, message session.Message) error {
	a.mu.Lock()
	incoming := current.incoming
	a.mu.Unlock()
	if incoming == nil || !incoming.accepted || incoming.id != message.OfferID {
		return errors.New("Peer sent file content without an accepted matching Transfer Offer")
	}
	entry := findManifestFile(incoming, message.Path)
	if entry == nil || message.Size < 0 || message.Size > streamChunkBytes || message.Offset < 0 || message.Offset > entry.Size || message.Size > entry.Size-message.Offset {
		return errors.New("Peer sent an invalid chunk for an unknown manifest file")
	}
	key := manifestPathKey(entry.Path)
	if incoming.fileOutcomes == nil {
		incoming.fileOutcomes = make(map[string]struct{}, incoming.manifest.FileCount)
	}
	if _, completed := incoming.fileOutcomes[key]; completed {
		return errors.New("Peer repeated an outcome for a manifest file")
	}
	if incoming.receivingFiles == nil {
		incoming.receivingFiles = make(map[string]*receivingFile, 4)
	}
	stream := incoming.receivingFiles[key]
	if stream == nil {
		if message.Offset != 0 {
			return errors.New("Peer started a file stream at a nonzero offset")
		}
		stream = a.startReceivingFile(incoming, entry)
		incoming.receivingFiles[key] = stream
	}
	if stream.received != message.Offset {
		return errors.New("Peer sent an overlapping or out-of-order file chunk")
	}
	var destination io.Writer = stream.digest
	if stream.destination != nil {
		destination = io.MultiWriter(stream.destination, stream.digest)
	}
	chunkStart := stream.received
	err := current.secure.ReceiveChunk(current.context, destination, message.Size, func(completed int64) {
		stream.progress(chunkStart + completed)
	})
	if err != nil {
		a.cleanupIncoming(current)
		return fmt.Errorf("receive %s: %w", entry.Path, err)
	}
	stream.received += message.Size
	if stream.destination != nil && stream.destination.failure != nil {
		stream.writeFailure = stream.destination.failure
	}
	return nil
}

func findManifestFile(incoming *incomingOffer, path string) *manifestEntry {
	for index := range incoming.manifest.Entries {
		entry := &incoming.manifest.Entries[index]
		if entry.Kind == manifestFile && entry.Path == path {
			return entry
		}
	}
	return nil
}

func (a *App) startReceivingFile(incoming *incomingOffer, entry *manifestEntry) *receivingFile {
	stream := &receivingFile{entry: entry, digest: sha256.New(), progress: a.progress("Receiving "+entry.Path, entry.Size)}
	stagingDirectory := filepath.Join(incoming.destination, ".transferly-staging")
	if err := rejectReparseAncestors(incoming.destination, stagingDirectory); err != nil {
		stream.writeFailure = fmt.Errorf("staging area became unsafe: %w", err)
		return stream
	}
	if err := os.MkdirAll(stagingDirectory, 0o700); err != nil {
		stream.writeFailure = fmt.Errorf("recreate staging area: %w", err)
		return stream
	}
	if err := rejectReparsePoint(stagingDirectory); err != nil {
		stream.writeFailure = fmt.Errorf("staging area became unsafe: %w", err)
		return stream
	}
	file, err := os.CreateTemp(stagingDirectory, "incoming-*.part")
	if err != nil {
		stream.writeFailure = fmt.Errorf("create temporary file: %w", err)
		return stream
	}
	stream.file = file
	stream.stagingPath = file.Name()
	stream.destination = &recoverableWriter{destination: file}
	return stream
}

func (a *App) completeIncomingFile(current *attempt, completion session.Message) error {
	a.mu.Lock()
	incoming := current.incoming
	a.mu.Unlock()
	if incoming == nil || !incoming.accepted || incoming.id != completion.OfferID {
		return errors.New("Peer completed a file without an accepted matching Transfer Offer")
	}
	entry := findManifestFile(incoming, completion.Path)
	key := manifestPathKey(completion.Path)
	stream := incoming.receivingFiles[key]
	if entry == nil || stream == nil || completion.Size != entry.Size || stream.received != entry.Size {
		return errors.New("Peer sent an invalid file completion frame")
	}
	if stream.file != nil {
		if err := stream.file.Sync(); err != nil && stream.writeFailure == nil {
			stream.writeFailure = fmt.Errorf("flush temporary file: %w", err)
		}
		if err := stream.file.Close(); err != nil && stream.writeFailure == nil {
			stream.writeFailure = fmt.Errorf("close temporary file: %w", err)
		}
		stream.file = nil
	}
	digest := hex.EncodeToString(stream.digest.Sum(nil))
	if stream.writeFailure != nil {
		return a.failIncomingFile(current, incoming, entry.Path, "destination write failed: "+stream.writeFailure.Error())
	}
	if len(completion.Digest) != 64 || !strings.EqualFold(completion.Digest, digest) || !strings.EqualFold(entry.Digest, digest) || completion.Success != nil && !*completion.Success {
		reason := completion.Reason
		if reason == "" {
			reason = "size or SHA-256 integrity check failed"
		}
		return a.failIncomingFile(current, incoming, entry.Path, reason)
	}
	finalPath := incomingPath(incoming, *entry)
	if err := ensurePathBeneath(incoming.destination, finalPath); err != nil {
		a.removeIncomingFiles(incoming)
		return err
	}
	if err := rejectReparseAncestors(incoming.destination, filepath.Dir(finalPath)); err != nil {
		a.removeIncomingFiles(incoming)
		return fmt.Errorf("final path became unsafe: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return err
	}
	if err := publishWithoutOverwrite(stream.stagingPath, finalPath); err != nil {
		return a.failIncomingFile(current, incoming, entry.Path, "destination write failed: final path became unavailable")
	}
	stream.stagingPath = ""
	delete(incoming.receivingFiles, key)
	if err := applyBasicMetadata(finalPath, *entry); err != nil {
		_ = os.Remove(finalPath)
		return a.failIncomingFile(current, incoming, entry.Path, "destination metadata write failed: "+err.Error())
	}
	incoming.fileOutcomes[key] = struct{}{}
	incoming.completedFile++
	success := true
	if err := current.secure.Send(session.Message{Type: "result", OfferID: incoming.id, Path: entry.Path, Success: &success}); err != nil {
		return err
	}
	a.line("Received %s (%d bytes) at %s.", entry.Path, entry.Size, finalPath)
	return nil
}

// recoverableWriter records the first destination failure but keeps reporting
// success to its caller so the stream is still drained completely. Returning
// the error here would desynchronize the wire, stranding the remaining bytes of
// this file in front of the next frame and failing the whole Transfer Offer.
// The recorded failure is surfaced afterwards, which is what lets independently
// verified files stay published when one file fails.
type recoverableWriter struct {
	destination io.Writer
	failure     error
}

//nolint:nilerr // Failures are recorded and reported after the stream drains.
func (w *recoverableWriter) Write(content []byte) (int, error) {
	if w.failure != nil {
		return len(content), nil
	}
	written, err := w.destination.Write(content)
	if err != nil {
		w.failure = err
		return len(content), nil
	}
	if written != len(content) {
		w.failure = io.ErrShortWrite
		return len(content), nil
	}
	return written, nil
}

func (a *App) failIncomingFile(current *attempt, incoming *incomingOffer, path, reason string) error {
	a.removeIncomingStream(incoming, path)
	if incoming.fileOutcomes == nil {
		incoming.fileOutcomes = make(map[string]struct{}, incoming.manifest.FileCount)
	}
	incoming.fileOutcomes[manifestPathKey(path)] = struct{}{}
	incoming.failedFile++
	success := false
	if err := current.secure.Send(session.Message{Type: "result", OfferID: incoming.id, Path: path, Success: &success, Reason: reason}); err != nil {
		return err
	}
	a.line("Transfer failed for %s: %s; incomplete content was removed and other files will continue.", path, reason)
	return nil
}

func isHexDigest(digest string) bool {
	_, err := hex.DecodeString(digest)
	return err == nil
}

func (a *App) receiveFileFailure(current *attempt, message session.Message) error {
	a.mu.Lock()
	incoming := current.incoming
	a.mu.Unlock()
	if incoming == nil || !incoming.accepted || incoming.id != message.OfferID || message.Path == "" || message.Reason == "" {
		return errors.New("Peer reported a file failure without an accepted matching Transfer Offer")
	}
	found := false
	for _, entry := range incoming.manifest.Entries {
		if entry.Kind == manifestFile && entry.Path == message.Path {
			found = true
			break
		}
	}
	if !found {
		return errors.New("Peer reported a failure for an unknown manifest file")
	}
	if incoming.fileOutcomes == nil {
		incoming.fileOutcomes = make(map[string]struct{}, incoming.manifest.FileCount)
	}
	key := manifestPathKey(message.Path)
	if _, completed := incoming.fileOutcomes[key]; completed {
		return errors.New("Peer repeated an outcome for a manifest file")
	}
	a.removeIncomingStream(incoming, message.Path)
	incoming.fileOutcomes[key] = struct{}{}
	incoming.failedFile++
	success := false
	if err := current.secure.Send(session.Message{Type: "result", OfferID: incoming.id, Path: message.Path, Success: &success, Reason: message.Reason}); err != nil {
		return err
	}
	a.line("Transfer failed for %s: %s; other files will continue.", message.Path, message.Reason)
	return nil
}

func (a *App) completeIncomingBatch(current *attempt, message session.Message) error {
	a.mu.Lock()
	incoming := current.incoming
	a.mu.Unlock()
	if incoming == nil || !incoming.accepted || incoming.id != message.OfferID || incoming.completedFile+incoming.failedFile != incoming.manifest.FileCount {
		return errors.New("Peer completed an inconsistent Transfer Offer")
	}
	for index := len(incoming.manifest.Entries) - 1; index >= 0; index-- {
		entry := incoming.manifest.Entries[index]
		if entry.Kind == manifestFolder {
			path := incomingPath(incoming, entry)
			if err := rejectReparseAncestors(incoming.destination, path); err != nil {
				return fmt.Errorf("manifest folder became unsafe: %w", err)
			}
			if err := applyBasicMetadata(path, entry); err != nil {
				return err
			}
		}
	}
	removeEmptyStagingDirectory(incoming.destination)
	success := incoming.failedFile == 0
	if err := current.secure.Send(session.Message{Type: "result", OfferID: incoming.id, Success: &success}); err != nil {
		return err
	}
	manifest := incoming.manifest
	a.clearIncoming(current, incoming)
	if incoming.failedFile > 0 {
		a.line("Transfer Offer partially completed: %d of %d files succeeded; %d failed.", incoming.completedFile, manifest.FileCount, incoming.failedFile)
	} else if manifest.FileCount != 1 || manifest.FolderCount != 0 {
		a.line("Received Transfer Offer: %d files, %d folders (%d bytes)", manifest.FileCount, manifest.FolderCount, manifest.TotalBytes)
	}
	return nil
}
