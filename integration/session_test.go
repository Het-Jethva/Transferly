package integration_test

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/Het-Jethva/Transferly/internal/terminal" // Invalidate black-box tests when the executable changes.
)

var (
	projectRoot                      string
	transferlyExecutable             string
	incompatibleTransferlyExecutable string
	corruptDigestExecutable          string
	shortIdleExecutable              string
	slowIdleExecutable               string
)

func TestMain(m *testing.M) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "locate integration test directory")
		os.Exit(1)
	}
	projectRoot = filepath.Dir(filepath.Dir(currentFile))

	tempDir, err := os.MkdirTemp("", "transferly-integration-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	transferlyExecutable = filepath.Join(tempDir, "transferly"+extension)
	incompatibleTransferlyExecutable = filepath.Join(tempDir, "transferly-v2"+extension)
	corruptDigestExecutable = filepath.Join(tempDir, "transferly-corrupt-digest"+extension)
	shortIdleExecutable = filepath.Join(tempDir, "transferly-short-idle"+extension)
	slowIdleExecutable = filepath.Join(tempDir, "transferly-slow-idle"+extension)

	if err := buildExecutable(transferlyExecutable); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildExecutable(incompatibleTransferlyExecutable, "-ldflags", "-X main.wireMajor=1"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildExecutable(corruptDigestExecutable, "-ldflags", "-X main.corruptDigest=true"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildExecutable(shortIdleExecutable, "-ldflags", "-X main.controllableTime=true"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildExecutable(slowIdleExecutable, "-ldflags", "-X main.controllableTime=true -X main.streamChunkDelay=40ms"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	exitCode := m.Run()
	if err := os.RemoveAll(tempDir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}

func buildExecutable(destination string, extraArguments ...string) error {
	arguments := []string{"build", "-o", destination}
	if raceEnabled {
		arguments = append(arguments, "-race")
	}
	arguments = append(arguments, extraArguments...)
	arguments = append(arguments, "./cmd/transferly")
	command := exec.Command("go", arguments...)
	command.Dir = projectRoot
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("build Transferly: %w\n%s", err, output)
	}
	return nil
}

func TestAcceptedFileIsPublishedWithoutChangingTheSource(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourceDirectory := t.TempDir()
	sourcePath := filepath.Join(sourceDirectory, "quarterly-report.txt")
	original := []byte("Transferly keeps the source unchanged.\n")
	if err := os.WriteFile(sourcePath, original, 0o640); err != nil {
		t.Fatal(err)
	}

	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: quarterly-report.txt (39 bytes)")
	destination := filepath.Join(receiver.homeDir, "Downloads", "quarterly-report.txt")
	receiver.waitFor(t, "Final path: "+destination)
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("content was written before approval: %v", err)
	}

	receiver.send(t, "accept")
	sender.waitFor(t, "Transfer complete: quarterly-report.txt (39 bytes)")
	receiver.waitFor(t, "Received quarterly-report.txt (39 bytes)")

	received, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(received) != string(original) {
		t.Fatalf("received bytes = %q, want %q", received, original)
	}
	unchanged, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(unchanged) != string(original) {
		t.Fatalf("source bytes changed to %q", unchanged)
	}
	if _, err := os.Stat(filepath.Join(receiver.homeDir, "Downloads", ".transferly-staging")); !os.IsNotExist(err) {
		t.Fatalf("temporary storage was retained after publication: %v", err)
	}
}

func TestLargeFileStreamsToCompletionWithConstrainedProcessMemory(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	const size = 32 * 1024 * 1024
	sourcePath := filepath.Join(t.TempDir(), "large.bin")
	source, err := os.Create(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 64*1024)
	for index := range chunk {
		chunk[index] = byte(index % 251)
	}
	for written := 0; written < size; written += len(chunk) {
		if _, err := source.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: large.bin (33554432 bytes)")
	receiver.send(t, "accept")
	sender.waitFor(t, "Sending large.bin: 33554432/33554432 bytes")
	receiver.waitFor(t, "Received large.bin (33554432 bytes)")

	destination := filepath.Join(receiver.homeDir, "Downloads", "large.bin")
	if fileSHA256(t, sourcePath) != fileSHA256(t, destination) {
		t.Fatal("large streamed destination digest does not match the source")
	}
}

func TestDigestMismatchRemovesIncompleteContent(t *testing.T) {
	sender := startPeer(t, corruptDigestExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "tampered.bin")
	if err := os.WriteFile(sourcePath, []byte("integrity matters"), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: tampered.bin (17 bytes)")
	receiver.send(t, "accept")
	receiver.waitFor(t, "SHA-256 integrity check failed; incomplete content was removed")
	sender.waitFor(t, "SHA-256 integrity check failed")

	downloads := filepath.Join(receiver.homeDir, "Downloads")
	if _, err := os.Stat(filepath.Join(downloads, "tampered.bin")); !os.IsNotExist(err) {
		t.Fatalf("mismatched content was published: %v", err)
	}
	if _, err := os.Stat(filepath.Join(downloads, ".transferly-staging")); !os.IsNotExist(err) {
		t.Fatalf("mismatched temporary content was retained: %v", err)
	}

	offersBefore := receiver.count("Transfer Offer: tampered.bin")
	sender.send(t, "send "+sourcePath)
	receiver.waitForCount(t, "Transfer Offer: tampered.bin", offersBefore+1)
	receiver.send(t, "reject")
}

func TestInvalidMissingAndUnsupportedSourcesDoNotEndTheSession(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sender.send(t, "send")
	sender.waitFor(t, "Usage: send <path>")
	missing := filepath.Join(t.TempDir(), "missing.txt")
	sender.send(t, "send "+missing)
	sender.waitFor(t, "Cannot offer")
	folder := t.TempDir()
	sender.send(t, "send "+folder)
	sender.waitFor(t, "only one readable regular file is supported")

	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "linked.txt")
	if err := os.Symlink(target, link); err == nil {
		unsupportedBefore := sender.count("only one readable regular file is supported")
		sender.send(t, "send "+link)
		sender.waitForCount(t, "only one readable regular file is supported", unsupportedBefore+1)
	}

	valid := filepath.Join(t.TempDir(), "valid.txt")
	if err := os.WriteFile(valid, []byte("still connected"), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+valid)
	receiver.waitFor(t, "Transfer Offer: valid.txt (15 bytes)")
	receiver.send(t, "reject")
	sender.waitFor(t, "Peer rejected Transfer Offer valid.txt")
}

func TestZeroByteFileIsIntegrityCheckedAndPublished(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "empty.dat")
	if err := os.WriteFile(sourcePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: empty.dat (0 bytes)")
	receiver.send(t, "accept")
	sender.waitFor(t, "Sending empty.dat: 0/0 bytes")
	receiver.waitFor(t, "Receiving empty.dat: 0/0 bytes")
	receiver.waitFor(t, "Received empty.dat (0 bytes)")

	destination := filepath.Join(receiver.homeDir, "Downloads", "empty.dat")
	info, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("zero-byte destination has size %d", info.Size())
	}
}

func TestExistingDestinationIsNotOverwrittenAndConflictNameIsPreviewed(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(sourcePath, []byte("new notes"), 0o600); err != nil {
		t.Fatal(err)
	}
	downloads := filepath.Join(receiver.homeDir, "Downloads")
	if err := os.MkdirAll(downloads, 0o755); err != nil {
		t.Fatal(err)
	}
	existingPath := filepath.Join(downloads, "notes.txt")
	if err := os.WriteFile(existingPath, []byte("existing notes"), 0o600); err != nil {
		t.Fatal(err)
	}

	sender.send(t, "send "+sourcePath)
	generatedPath := filepath.Join(downloads, "notes (1).txt")
	receiver.waitFor(t, "Final path: "+generatedPath)
	receiver.send(t, "accept")
	receiver.waitFor(t, "Received notes.txt (9 bytes)")

	existing, err := os.ReadFile(existingPath)
	if err != nil || string(existing) != "existing notes" {
		t.Fatalf("existing destination changed: bytes=%q err=%v", existing, err)
	}
	generated, err := os.ReadFile(generatedPath)
	if err != nil || string(generated) != "new notes" {
		t.Fatalf("generated destination: bytes=%q err=%v", generated, err)
	}
}

func TestReceiverOverridesDestinationForOneOffer(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "photo.jpg")
	if err := os.WriteFile(sourcePath, []byte("image bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	customDestination := filepath.Join(receiver.workDir, "Chosen Folder")
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: photo.jpg (11 bytes)")
	receiver.send(t, `destination "`+customDestination+`"`)
	receiver.waitFor(t, "Destination updated for this Transfer Offer only.")
	customPath := filepath.Join(customDestination, "photo.jpg")
	receiver.waitFor(t, "Final path: "+customPath)
	receiver.send(t, "accept")
	receiver.waitFor(t, "Received photo.jpg (11 bytes)")
	sender.waitFor(t, "Transfer complete: photo.jpg (11 bytes)")

	if _, err := os.Stat(customPath); err != nil {
		t.Fatalf("custom destination was not published: %v", err)
	}
	defaultPath := filepath.Join(receiver.homeDir, "Downloads", "photo.jpg")
	if _, err := os.Stat(defaultPath); !os.IsNotExist(err) {
		t.Fatalf("offer unexpectedly used the default destination: %v", err)
	}

	defaultPreview := "Destination: " + filepath.Join(receiver.homeDir, "Downloads")
	previewsBefore := receiver.count(defaultPreview)
	sender.send(t, "send "+sourcePath)
	receiver.waitForCount(t, defaultPreview, previewsBefore+1)
	receiver.send(t, "reject")
}

func TestRejectedFileWritesNothingAndLeavesTheSessionActive(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "private.txt")
	if err := os.WriteFile(sourcePath, []byte("not approved"), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: private.txt (12 bytes)")
	receiver.send(t, "reject")
	receiver.waitFor(t, "Transfer Offer rejected. No file content was written.")
	sender.waitFor(t, "Peer rejected Transfer Offer private.txt. No file content was sent.")

	destination := filepath.Join(receiver.homeDir, "Downloads", "private.txt")
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("rejected content exists at %s: %v", destination, err)
	}

	offersBefore := receiver.count("Transfer Offer: private.txt")
	sender.send(t, "send "+sourcePath)
	receiver.waitForCount(t, "Transfer Offer: private.txt", offersBefore+1)
	receiver.send(t, "reject")
}

func TestSimultaneousBidirectionalOffersAreSerialized(t *testing.T) {
	first := startPeer(t, transferlyExecutable)
	second := startPeer(t, transferlyExecutable)
	verifyPeers(t, first, second)

	firstSource := filepath.Join(t.TempDir(), "from-first.txt")
	secondSource := filepath.Join(t.TempDir(), "from-second.txt")
	if err := os.WriteFile(firstSource, []byte("first Peer"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondSource, []byte("second Peer"), 0o600); err != nil {
		t.Fatal(err)
	}

	first.send(t, "send "+firstSource)
	second.send(t, "send "+secondSource)

	firstOffer := "Transfer Offer: from-second.txt"
	secondOffer := "Transfer Offer: from-first.txt"
	firstPresented := waitForEither(t, first, firstOffer, second, secondOffer)
	if firstPresented {
		first.send(t, "accept")
		first.waitFor(t, "Received from-second.txt")
		second.waitFor(t, secondOffer)
		second.send(t, "accept")
	} else {
		second.send(t, "accept")
		second.waitFor(t, "Received from-first.txt")
		first.waitFor(t, firstOffer)
		first.send(t, "accept")
	}
	first.waitFor(t, "Received from-second.txt")
	second.waitFor(t, "Received from-first.txt")

	if bytes, err := os.ReadFile(filepath.Join(first.homeDir, "Downloads", "from-second.txt")); err != nil || string(bytes) != "second Peer" {
		t.Fatalf("first Peer destination: bytes=%q err=%v", bytes, err)
	}
	if bytes, err := os.ReadFile(filepath.Join(second.homeDir, "Downloads", "from-first.txt")); err != nil || string(bytes) != "first Peer" {
		t.Fatalf("second Peer destination: bytes=%q err=%v", bytes, err)
	}
}

func TestSendingAndReceivingRolesAlternateWithoutReconnecting(t *testing.T) {
	first := startPeer(t, transferlyExecutable)
	second := startPeer(t, transferlyExecutable)
	verifyPeers(t, first, second)

	firstSource := filepath.Join(t.TempDir(), "first-to-second.txt")
	secondSource := filepath.Join(t.TempDir(), "second-to-first.txt")
	if err := os.WriteFile(firstSource, []byte("forward"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondSource, []byte("backward"), 0o600); err != nil {
		t.Fatal(err)
	}

	first.send(t, "send "+firstSource)
	second.waitFor(t, "Transfer Offer: first-to-second.txt")
	second.send(t, "accept")
	first.waitFor(t, "Transfer complete: first-to-second.txt")

	second.send(t, "send "+secondSource)
	first.waitFor(t, "Transfer Offer: second-to-first.txt")
	first.send(t, "accept")
	second.waitFor(t, "Transfer complete: second-to-first.txt")
}

func TestLaterTransferOffersWaitInSubmissionOrder(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	directory := t.TempDir()
	first := filepath.Join(directory, "first.txt")
	second := filepath.Join(directory, "second.txt")
	third := filepath.Join(directory, "third.txt")
	for path, content := range map[string]string{
		first: "first", second: "second", third: "third",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	sender.send(t, "send "+first)
	receiver.waitFor(t, "Transfer Offer: first.txt")
	sender.send(t, "send "+second)
	sender.waitFor(t, "Transfer Offer queued: second.txt")
	sender.send(t, "send "+third)
	sender.waitFor(t, "Transfer Offer queued: third.txt")
	if receiver.contains("Transfer Offer: second.txt") || receiver.contains("Transfer Offer: third.txt") {
		t.Fatal("a later Transfer Offer was presented before the active offer completed")
	}

	receiver.send(t, "accept")
	receiver.waitFor(t, "Received first.txt")
	receiver.waitFor(t, "Transfer Offer: second.txt")
	receiver.send(t, "reject")
	sender.waitFor(t, "Peer rejected Transfer Offer second.txt")
	receiver.waitFor(t, "Transfer Offer: third.txt")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Received third.txt")

	downloads := filepath.Join(receiver.homeDir, "Downloads")
	for _, name := range []string{"first.txt", "third.txt"} {
		if _, err := os.Stat(filepath.Join(downloads, name)); err != nil {
			t.Fatalf("queued offer %s was not published: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(downloads, "second.txt")); !os.IsNotExist(err) {
		t.Fatalf("rejected queued offer was published: %v", err)
	}
}

func TestTransferOfferQueuePreservesOrderAcrossPeers(t *testing.T) {
	first := startPeer(t, transferlyExecutable)
	coordinator := startPeer(t, transferlyExecutable)
	verifyPeers(t, first, coordinator)

	directory := t.TempDir()
	active := filepath.Join(directory, "active-from-first.txt")
	earlier := filepath.Join(directory, "earlier-from-first.txt")
	later := filepath.Join(directory, "later-from-coordinator.txt")
	for _, path := range []string{active, earlier, later} {
		if err := os.WriteFile(path, []byte(filepath.Base(path)), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	first.send(t, "send "+active)
	coordinator.waitFor(t, "Transfer Offer: active-from-first.txt")
	first.send(t, "send "+earlier)
	first.waitFor(t, "Transfer Offer queued: earlier-from-first.txt")
	coordinator.send(t, "send "+later)
	coordinator.waitFor(t, "Transfer Offer queued: later-from-coordinator.txt")

	coordinator.send(t, "reject")
	coordinator.waitFor(t, "Transfer Offer: earlier-from-first.txt")
	if first.contains("Transfer Offer: later-from-coordinator.txt") {
		t.Fatal("a later offer from the coordinating Peer overtook an earlier queued offer")
	}
	coordinator.send(t, "reject")
	first.waitFor(t, "Transfer Offer: later-from-coordinator.txt")
	first.send(t, "reject")
}

func TestQueuedOfferIsAcknowledgedAcrossCompletionBoundary(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	directory := t.TempDir()
	active := filepath.Join(directory, "slow-active.bin")
	queued := filepath.Join(directory, "after-completion.txt")
	if err := os.WriteFile(active, make([]byte, 1024*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(queued, []byte("next"), 0o600); err != nil {
		t.Fatal(err)
	}

	sender.send(t, "send "+active)
	receiver.waitFor(t, "Transfer Offer: slow-active.bin")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Receiving slow-active.bin: 0/1048576 bytes")
	sender.send(t, "send "+queued)
	sender.waitFor(t, "Transfer Offer queued: after-completion.txt")
	receiver.waitFor(t, "Received slow-active.bin")
	receiver.waitFor(t, "Transfer Offer: after-completion.txt")
	receiver.send(t, "reject")
}

func TestTransferOfferQueueHasABoundedCapacity(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "bounded.txt")
	if err := os.WriteFile(sourcePath, []byte("bounded"), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: bounded.txt")
	for index := 0; index < 64; index++ {
		sender.send(t, "send "+sourcePath)
	}
	sender.waitForCount(t, "Transfer Offer queued: bounded.txt", 64)
	sender.send(t, "send "+sourcePath)
	sender.waitFor(t, "Transfer Offer queue is full")
}

func TestDisconnectDuringOfferAcceptanceCleansStaging(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "disconnecting.bin")
	if err := os.WriteFile(sourcePath, make([]byte, 1024*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: disconnecting.bin")
	receiver.send(t, "accept")
	receiver.send(t, "disconnect")
	receiver.waitFor(t, "Transfer Session ended.")
	sender.waitFor(t, "Transfer Session ended.")

	downloads := filepath.Join(receiver.homeDir, "Downloads")
	if _, err := os.Stat(filepath.Join(downloads, ".transferly-staging")); !os.IsNotExist(err) {
		t.Fatalf("staging remained after disconnect raced offer acceptance: %v", err)
	}
}

func TestQueuedOffersDisappearWhenTheTransferSessionEnds(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	directory := t.TempDir()
	active := filepath.Join(directory, "active.txt")
	queued := filepath.Join(directory, "queued.txt")
	fresh := filepath.Join(directory, "fresh.txt")
	for _, path := range []string{active, queued, fresh} {
		if err := os.WriteFile(path, []byte(filepath.Base(path)), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	sender.send(t, "send "+active)
	receiver.waitFor(t, "Transfer Offer: active.txt")
	sender.send(t, "send "+queued)
	sender.waitFor(t, "Transfer Offer queued: queued.txt")
	sender.send(t, "disconnect")
	sender.waitFor(t, "Transfer Session ended.")
	receiver.waitFor(t, "Transfer Session ended.")

	verifyPeers(t, sender, receiver)
	sender.send(t, "send "+fresh)
	receiver.waitFor(t, "Transfer Offer: fresh.txt")
	if receiver.contains("Transfer Offer: queued.txt") {
		t.Fatal("a queued Transfer Offer survived the previous Transfer Session")
	}
	receiver.send(t, "reject")
}

func TestPeersVerifyDisconnectAndVerifyAgain(t *testing.T) {
	first := startPeer(t, transferlyExecutable)
	second := startPeer(t, transferlyExecutable)

	verifyPeers(t, first, second)

	first.send(t, "connect "+second.endpoint)
	first.waitFor(t, "Already connected to a Peer")

	first.send(t, "disconnect")
	first.waitFor(t, "Transfer Session ended")
	second.waitFor(t, "Transfer Session ended")

	verifyPeers(t, first, second)
}

func TestVerificationMismatchClosesTheConnection(t *testing.T) {
	first := startPeer(t, transferlyExecutable)
	second := startPeer(t, transferlyExecutable)

	first.send(t, "connect "+second.endpoint)
	firstCode := first.waitForCode(t, 0)
	secondCode := second.waitForCode(t, 0)
	if firstCode != secondCode {
		t.Fatalf("Peers displayed different verification codes: %s and %s", firstCode, secondCode)
	}

	first.send(t, "no")
	first.waitFor(t, "Verification did not match; connection closed")
	second.waitFor(t, "Peer rejected verification; connection closed")
}

func TestIncompatibleProtocolMajorIsRejectedBeforeVerification(t *testing.T) {
	first := startPeer(t, transferlyExecutable)
	second := startPeer(t, incompatibleTransferlyExecutable)

	first.send(t, "connect "+second.endpoint)
	first.waitFor(t, "Incompatible wire protocol: local 2.0, Peer 1.0")
	second.waitFor(t, "Incompatible wire protocol: local 1.0, Peer 2.0")

	if first.contains("Verification code:") || second.contains("Verification code:") {
		t.Fatal("an incompatible connection reached human verification")
	}
}

func TestActiveTransferIsExemptFromIdleExpiry(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiver := startPeer(t, slowIdleExecutable)

	sourcePath := filepath.Join(t.TempDir(), "deliberately-slow.bin")
	if err := os.WriteFile(sourcePath, make([]byte, 1024*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	verifyPeers(t, sender, receiver)
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: deliberately-slow.bin")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Receiving deliberately-slow.bin: 0/1048576 bytes")
	sender.send(t, "advance-time 1h")
	receiver.send(t, "advance-time 1h")
	sender.waitFor(t, "Transfer complete: deliberately-slow.bin")
	receiver.waitFor(t, "Received deliberately-slow.bin")

	if sender.contains("Transfer Session expired") || receiver.contains("Transfer Session expired") {
		t.Fatal("an active Transfer Offer expired for session idleness")
	}
	sender.send(t, "advance-time 15m")
	receiver.send(t, "advance-time 15m")
	waitForEither(t, sender, "Transfer Session expired after 15m0s", receiver, "Transfer Session expired after 15m0s")
}

func TestIdleWarningKeepAliveAndExpiryUseControllableTime(t *testing.T) {
	first := startPeer(t, shortIdleExecutable)
	second := startPeer(t, shortIdleExecutable)
	verifyPeers(t, first, second)

	warning := "Transfer Session idle for 14m0s"
	first.send(t, "advance-time 14m")
	second.send(t, "advance-time 14m")
	first.waitFor(t, warning)
	second.waitFor(t, warning)

	commandsBefore := first.count("Commands: connect")
	first.send(t, "help")
	first.waitForCount(t, "Commands: connect", commandsBefore+1)
	second.waitFor(t, "Test clock: Peer terminal activity observed.")
	firstWarnings := first.count(warning)
	secondWarnings := second.count(warning)
	first.send(t, "advance-time 14m")
	second.send(t, "advance-time 14m")
	first.waitForCount(t, warning, firstWarnings+1)
	second.waitForCount(t, warning, secondWarnings+1)
	if first.contains("Transfer Session ended.") || second.contains("Transfer Session ended.") {
		t.Fatal("the Peer expired despite terminal activity in the Transfer Session")
	}

	first.send(t, "keep-alive")
	first.waitFor(t, "Transfer Session kept alive.")
	second.waitFor(t, "Peer kept the Transfer Session alive.")
	firstWarnings = first.count(warning)
	secondWarnings = second.count(warning)
	first.send(t, "advance-time 14m")
	second.send(t, "advance-time 14m")
	first.waitForCount(t, warning, firstWarnings+1)
	second.waitForCount(t, warning, secondWarnings+1)
	if first.contains("Transfer Session ended.") || second.contains("Transfer Session ended.") {
		t.Fatal("the Transfer Session expired before the keep-alive interval elapsed")
	}

	first.send(t, "advance-time 1m")
	second.send(t, "advance-time 1m")
	expired := "Transfer Session expired after 15m0s without activity"
	waitForEither(t, first, expired, second, expired)
	first.waitFor(t, "Transfer Session ended.")
	second.waitFor(t, "Transfer Session ended.")

	verifyPeers(t, first, second)
}

func TestAdditionalConnectionsReceiveAClearBusyOutcome(t *testing.T) {
	first := startPeer(t, transferlyExecutable)
	second := startPeer(t, transferlyExecutable)
	additional := startPeer(t, transferlyExecutable)

	first.send(t, "connect "+second.endpoint)
	firstCode := first.waitForCode(t, 0)
	secondCode := second.waitForCode(t, 0)
	if firstCode != secondCode {
		t.Fatalf("Peers displayed different verification codes: %s and %s", firstCode, secondCode)
	}

	additional.send(t, "connect "+second.endpoint)
	additional.waitFor(t, "Peer is busy with another active or pending Transfer Session")
	if additional.contains("Verification code:") {
		t.Fatal("a busy connection created verification state")
	}

	first.send(t, "yes")
	second.send(t, "yes")
	first.waitFor(t, "Transfer Session verified")
	second.waitFor(t, "Transfer Session verified")

	busyBefore := additional.count("Peer is busy with another active or pending Transfer Session")
	additional.send(t, "connect "+second.endpoint)
	additional.waitForCount(t, "Peer is busy with another active or pending Transfer Session", busyBefore+1)
}

func TestConnectionErrorsLeaveThePeerAvailable(t *testing.T) {
	first := startPeer(t, transferlyExecutable)
	second := startPeer(t, transferlyExecutable)

	first.send(t, "connect localhost:1234")
	first.waitFor(t, "Invalid endpoint")

	first.send(t, "connect 127.0.0.1:1")
	first.waitFor(t, "Unable to connect")

	verifyPeers(t, first, second)
}

func TestQuitExitsCleanlyWithoutSavingTrust(t *testing.T) {
	peer := startPeer(t, transferlyExecutable)
	peer.send(t, "quit")
	peer.waitFor(t, "Transferly stopped. No identity or trust was saved.")
	if err := peer.command.Wait(); err != nil {
		t.Fatalf("quit returned an error: %v\noutput:\n%s", err, peer.snapshot())
	}
	entries, err := os.ReadDir(peer.workDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("Transferly persisted process state in %s: %v", peer.workDir, entries)
	}
}

func verifyPeers(t *testing.T, first, second *peerProcess) {
	t.Helper()
	firstCodeCount := first.matchCount(verificationCodePattern)
	secondCodeCount := second.matchCount(verificationCodePattern)

	first.send(t, "connect "+second.endpoint)
	firstCode := first.waitForCode(t, firstCodeCount)
	secondCode := second.waitForCode(t, secondCodeCount)
	if firstCode != secondCode {
		t.Fatalf("Peers displayed different verification codes: %s and %s", firstCode, secondCode)
	}

	firstVerifiedCount := first.count("Transfer Session verified")
	secondVerifiedCount := second.count("Transfer Session verified")
	first.send(t, "yes")
	second.send(t, "yes")
	first.waitForCount(t, "Transfer Session verified", firstVerifiedCount+1)
	second.waitForCount(t, "Transfer Session verified", secondVerifiedCount+1)
}

var (
	endpointPattern         = regexp.MustCompile(`Endpoint: (127\.0\.0\.1:\d+)`)
	verificationCodePattern = regexp.MustCompile(`Verification code: (\d{6})`)
)

type peerProcess struct {
	command  *exec.Cmd
	stdin    io.WriteCloser
	endpoint string
	workDir  string
	homeDir  string

	mu     sync.Mutex
	output strings.Builder
	change chan struct{}
}

func startPeer(t *testing.T, executable string) *peerProcess {
	t.Helper()
	workDir := t.TempDir()
	homeDir := t.TempDir()
	command := exec.Command(executable, "--listen", "127.0.0.1:0")
	command.Dir = workDir
	command.Env = append(os.Environ(), "HOME="+homeDir, "USERPROFILE="+homeDir, "GOMEMLIMIT=24MiB")
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = command.Stdout

	peer := &peerProcess{command: command, stdin: stdin, workDir: workDir, homeDir: homeDir, change: make(chan struct{}, 1)}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.stop(t) })

	go peer.collect(stdout)
	endpointLine := peer.waitFor(t, "Endpoint: 127.0.0.1:")
	match := endpointPattern.FindStringSubmatch(endpointLine)
	if match == nil {
		t.Fatalf("could not parse endpoint from output:\n%s", peer.snapshot())
	}
	peer.endpoint = match[1]
	return peer
}

func fileSHA256(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func (p *peerProcess) collect(reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		p.mu.Lock()
		p.output.WriteString(scanner.Text())
		p.output.WriteByte('\n')
		p.mu.Unlock()
		select {
		case p.change <- struct{}{}:
		default:
		}
	}
}

func (p *peerProcess) send(t *testing.T, line string) {
	t.Helper()
	if _, err := io.WriteString(p.stdin, line+"\n"); err != nil {
		t.Fatalf("send %q: %v\noutput:\n%s", line, err, p.snapshot())
	}
}

func waitForEither(t *testing.T, first *peerProcess, firstText string, second *peerProcess, secondText string) bool {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if first.contains(firstText) {
			return true
		}
		if second.contains(secondText) {
			return false
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for either %q or %q\nfirst output:\n%s\nsecond output:\n%s", firstText, secondText, first.snapshot(), second.snapshot())
		}
	}
}

func (p *peerProcess) waitFor(t *testing.T, text string) string {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		output := p.snapshot()
		if strings.Contains(output, text) {
			return output
		}
		select {
		case <-p.change:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %q\noutput:\n%s", text, output)
		}
	}
}

func (p *peerProcess) waitForCount(t *testing.T, text string, expected int) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		output := p.snapshot()
		if strings.Count(output, text) >= expected {
			return
		}
		select {
		case <-p.change:
		case <-deadline.C:
			t.Fatalf("timed out waiting for occurrence %d of %q\noutput:\n%s", expected, text, output)
		}
	}
}

func (p *peerProcess) waitForCode(t *testing.T, previousMatches int) string {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		output := p.snapshot()
		matches := verificationCodePattern.FindAllStringSubmatch(output, -1)
		if len(matches) > previousMatches {
			return matches[previousMatches][1]
		}
		select {
		case <-p.change:
		case <-deadline.C:
			t.Fatalf("timed out waiting for verification code\noutput:\n%s", output)
		}
	}
}

func (p *peerProcess) snapshot() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.output.String()
}

func (p *peerProcess) contains(text string) bool {
	return strings.Contains(p.snapshot(), text)
}

func (p *peerProcess) count(text string) int {
	return strings.Count(p.snapshot(), text)
}

func (p *peerProcess) matchCount(pattern *regexp.Regexp) int {
	return len(pattern.FindAllString(p.snapshot(), -1))
}

func (p *peerProcess) stop(t *testing.T) {
	t.Helper()
	if p.command.Process == nil || p.command.ProcessState != nil {
		return
	}
	_, _ = io.WriteString(p.stdin, "quit\n")
	done := make(chan error, 1)
	go func() {
		done <- p.command.Wait()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Transferly did not exit cleanly: %v\noutput:\n%s", err, p.snapshot())
		}
	case <-time.After(2 * time.Second):
		_ = p.command.Process.Kill()
		<-done
		t.Errorf("Transferly did not exit within two seconds\noutput:\n%s", p.snapshot())
	}
}
