package integration_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Het-Jethva/Transferly/internal/session"
	_ "github.com/Het-Jethva/Transferly/internal/terminal" // Invalidate black-box tests when the executable changes.
)

var (
	projectRoot                      string
	transferlyExecutable             string
	incompatibleTransferlyExecutable string
	corruptDigestExecutable          string
	shortIdleExecutable              string
	slowIdleExecutable               string
	coverageDirectory                string
)

// coverageEnabled reports whether this run should collect coverage from the
// Transferly processes the suite launches. Because the suite exercises the
// product through real executables, ordinary -coverprofile accounting sees
// nothing; the binaries are instead built with -cover and each process writes
// its own profile into GOCOVERDIR for later merging with `go tool covdata`.
func coverageEnabled() bool { return coverageDirectory != "" }

func TestMain(m *testing.M) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "locate integration test directory")
		os.Exit(1)
	}
	projectRoot = filepath.Dir(filepath.Dir(currentFile))

	if requested := os.Getenv("TRANSFERLY_COVERDIR"); requested != "" {
		absolute, err := filepath.Abs(requested)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := os.MkdirAll(absolute, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		coverageDirectory = absolute
	}

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
	if err := buildFaultExecutable(corruptDigestExecutable, "-ldflags", "-X main.corruptDigest=true"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildFaultExecutable(shortIdleExecutable, "-ldflags", "-X main.controllableTime=true"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildFaultExecutable(slowIdleExecutable, "-ldflags", "-X main.controllableTime=true -X main.streamChunkDelay=40ms"); err != nil {
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

// buildFaultExecutable builds the injectable variant. Fault behavior lives
// behind the transferly_faults build tag so the default executable under test
// -- and every default build -- contains no fault branch at all.
func buildFaultExecutable(destination string, extraArguments ...string) error {
	arguments := append([]string{"-tags", "transferly_faults"}, extraArguments...)
	return buildExecutable(destination, arguments...)
}

func buildExecutable(destination string, extraArguments ...string) error {
	arguments := []string{"build", "-o", destination}
	if raceEnabled {
		arguments = append(arguments, "-race")
	}
	if coverageEnabled() {
		arguments = append(arguments, "-cover", "-coverpkg=github.com/Het-Jethva/Transferly/...")
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

func TestProductHelpAndVersionDescribeTheStatelessTerminalExperience(t *testing.T) {
	helpCommand := exec.Command(transferlyExecutable, "--help")
	help, err := helpCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("--help: %v\n%s", err, help)
	}
	for _, expected := range []string{
		"discover Available Peers",
		"connect <peer-number|IPv4:port>",
		"compare the six-digit verification code",
		"send <path>...",
		"details",
		"destination <path>",
		"cancel",
		"disconnect",
		"keep-alive",
		"quit",
		"--name",
		"--output",
		"--listen",
		"--version",
		"No configuration, identity, trust, history, logs, telemetry, relay, or update checks are created.",
	} {
		if !strings.Contains(string(help), expected) {
			t.Errorf("--help did not contain %q:\n%s", expected, help)
		}
	}

	versionCommand := exec.Command(transferlyExecutable, "--version")
	version, err := versionCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("--version: %v\n%s", err, version)
	}
	if !strings.Contains(string(version), "Transferly dev") || !strings.Contains(string(version), "Wire protocol 3.0") {
		t.Fatalf("--version did not report both versions:\n%s", version)
	}
}

func TestTemporaryNameAndOutputFlagsApplyOnlyToTheCurrentProcess(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	customOutput := filepath.Join(t.TempDir(), "Current Run Output")
	receiver := startPeerWithArguments(t, transferlyExecutable, t.TempDir(), "--listen", "127.0.0.1:0", "--name", "TEMPORARY-PEER", "--output", customOutput)
	receiver.waitFor(t, "Peer name: TEMPORARY-PEER")
	receiver.waitFor(t, "Default incoming destination: "+customOutput+" (this run only)")
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "flag-output.txt")
	if err := os.WriteFile(sourcePath, []byte("temporary override"), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Destination: "+customOutput)
	receiver.send(t, "accept")
	receiver.waitFor(t, "Received flag-output.txt")
	assertFileContent(t, filepath.Join(customOutput, "flag-output.txt"), "temporary override")
}

func TestMixedFilesAndFoldersArePublishedAsOneTransferOffer(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	source := t.TempDir()
	folder := filepath.Join(source, "Project Ω")
	if err := os.MkdirAll(filepath.Join(folder, "nested", "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	notesSource := filepath.Join(folder, "nested", "notes.txt")
	if err := os.WriteFile(notesSource, []byte("nested notes"), 0o600); err != nil {
		t.Fatal(err)
	}
	modified := time.Date(2024, time.January, 2, 3, 4, 6, 0, time.Local)
	if err := os.Chtimes(notesSource, modified, modified); err != nil {
		t.Fatal(err)
	}
	if err := addFileAttributes(notesSource, testFileAttributeReadOnly); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(notesSource, 0o600) })
	hiddenSource := filepath.Join(folder, ".hidden")
	if err := os.WriteFile(hiddenSource, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := addFileAttributes(hiddenSource, testFileAttributeHidden); err != nil {
		t.Fatal(err)
	}
	standalone := filepath.Join(source, "standalone.txt")
	if err := os.WriteFile(standalone, []byte("standalone"), 0o600); err != nil {
		t.Fatal(err)
	}

	sender.send(t, `send "`+folder+`" "`+standalone+`"`)
	receiver.waitFor(t, "Transfer Offer: 2 top-level roots, 3 files, 3 folders (22 bytes)")
	receiver.waitFor(t, "Top-level roots: Project Ω, standalone.txt")
	receiver.waitFor(t, "Choose accept, reject, destination <path>, or details.")
	receiver.send(t, "details")
	receiver.waitFor(t, "folder Project Ω/nested/empty")
	receiver.waitFor(t, "file Project Ω/.hidden (0 bytes)")
	receiver.send(t, "accept")
	sender.waitFor(t, "Transfer complete: 3 files, 3 folders (22 bytes).")
	receiver.waitFor(t, "Received Transfer Offer: 3 files, 3 folders (22 bytes)")

	downloads := filepath.Join(receiver.homeDir, "Downloads")
	notesDestination := filepath.Join(downloads, "Project Ω", "nested", "notes.txt")
	assertFileContent(t, notesDestination, "nested notes")
	t.Cleanup(func() { _ = os.Chmod(notesDestination, 0o600) })
	notesInfo, err := os.Stat(notesDestination)
	if err != nil || notesInfo.ModTime().Sub(modified).Abs() > 2*time.Second {
		t.Fatalf("last-modified timestamp was not preserved: info=%v err=%v", notesInfo, err)
	}
	if preserved, err := fileHasAttributes(notesDestination, testFileAttributeReadOnly); err != nil || !preserved {
		t.Fatalf("read-only attribute was not preserved: preserved=%v err=%v", preserved, err)
	}
	hiddenDestination := filepath.Join(downloads, "Project Ω", ".hidden")
	assertFileContent(t, hiddenDestination, "")
	if preserved, err := fileHasAttributes(hiddenDestination, testFileAttributeHidden); err != nil || !preserved {
		t.Fatalf("hidden attribute was not preserved: preserved=%v err=%v", preserved, err)
	}
	assertFileContent(t, filepath.Join(downloads, "standalone.txt"), "standalone")
	if info, err := os.Stat(filepath.Join(downloads, "Project Ω", "nested", "empty")); err != nil || !info.IsDir() {
		t.Fatalf("empty folder was not published: info=%v err=%v", info, err)
	}
}

func TestMalformedAndTruncatedWireStreamsFailClosedAndLeaveThePeerAvailable(t *testing.T) {
	receiver := startPeer(t, transferlyExecutable)
	failure := "Could not establish a secure Transfer Session"
	for index, payload := range [][]byte{
		[]byte("{not-a-tls-or-protocol-frame}\n"),
		{0x16, 0x03, 0x03, 0x00, 0x20, 0x01, 0x02}, // truncated TLS record
	} {
		connection, err := net.Dial("tcp4", receiver.endpoint)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Write(payload); err != nil {
			connection.Close()
			t.Fatal(err)
		}
		if err := connection.Close(); err != nil {
			t.Fatal(err)
		}
		receiver.waitForCount(t, failure, index+1)
	}

	assertNoProcessFiles(t, receiver)

	other := startPeer(t, transferlyExecutable)
	verifyPeers(t, other, receiver)
}

func TestHostilePeerManifestPathsEndTheSessionBeforeContentIsAccepted(t *testing.T) {
	tests := map[string]string{
		"../escape.txt":          "traversal segment",
		"/absolute.txt":          "absolute path",
		"folder/CON.txt":         "Windows reserved name",
		"folder/trailing.txt. ":  "trailing dot or space",
		"folder/file.txt:stream": "colon",
	}
	for path, reason := range tests {
		t.Run(reason, func(t *testing.T) {
			receiver := startPeer(t, transferlyExecutable)
			hostile := openHostileSession(t, receiver)
			if err := hostile.Send(session.Message{Type: "offer", OfferID: strings.Repeat("a", 32), RootCount: 1, FileCount: 1, TotalBytes: 1}); err != nil {
				t.Fatal(err)
			}
			if err := hostile.Send(session.Message{Type: "manifest-entry", OfferID: strings.Repeat("a", 32), Path: path, Kind: "file", Size: 1, Digest: strings.Repeat("0", 64)}); err != nil {
				t.Fatal(err)
			}
			receiver.waitFor(t, reason)
			receiver.waitFor(t, "Transfer Session ended after a connection error")

			assertNoProcessFiles(t, receiver)
		})
	}
}

func TestHostilePeerOversizedMetadataEndsTheSessionWithoutDestinationWrites(t *testing.T) {
	receiver := startPeer(t, transferlyExecutable)
	hostile := openHostileSession(t, receiver)
	if err := hostile.Send(session.Message{
		Type: "offer", OfferID: strings.Repeat("c", 32), RootCount: 1, FileCount: 100_001, TotalBytes: 1,
	}); err != nil {
		t.Fatal(err)
	}
	receiver.waitFor(t, "invalid or oversized Transfer Offer header")
	receiver.waitFor(t, "Transfer Session ended after a connection error")

	assertNoProcessFiles(t, receiver)
}

func TestHostilePeerUnicodeCaseAliasesAreRejected(t *testing.T) {
	receiver := startPeer(t, transferlyExecutable)
	hostile := openHostileSession(t, receiver)
	offerID := strings.Repeat("b", 32)
	if err := hostile.Send(session.Message{Type: "offer", OfferID: offerID, RootCount: 2, FileCount: 2, TotalBytes: 2}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"Résumé.txt", "RE\u0301SUME\u0301.TXT"} {
		if err := hostile.Send(session.Message{Type: "manifest-entry", OfferID: offerID, Path: path, Kind: "file", Size: 1, Digest: strings.Repeat("0", 64)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := hostile.Send(session.Message{Type: "offer-complete", OfferID: offerID}); err != nil {
		t.Fatal(err)
	}
	receiver.waitFor(t, "case-insensitive or Unicode alias")
	receiver.waitFor(t, "Transfer Session ended after a connection error")
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

func TestAdaptiveBatchProgressReportsBoundedConcurrentFilesAndFinalItems(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	folder := filepath.Join(t.TempDir(), "concurrent-batch")
	if err := os.Mkdir(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 5; index++ {
		path := filepath.Join(folder, fmt.Sprintf("file-%d.bin", index))
		if err := os.WriteFile(path, make([]byte, 256*1024), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	sender.send(t, "send "+folder)
	receiver.waitFor(t, "Transfer Offer: 1 top-level roots, 5 files")
	receiver.send(t, "accept")
	sender.waitFor(t, "Adaptive scheduling: up to 4 concurrent file streams")
	sender.waitFor(t, "Active files:")
	sender.waitFor(t, "Aggregate:")
	sender.waitFor(t, "Rate:")
	sender.waitFor(t, "ETA:")
	sender.waitFor(t, "Item succeeded: concurrent-batch/file-1.bin")
	sender.waitFor(t, "Transfer complete: 5 files, 1 folders")
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
	sender.waitFor(t, "Current: large.bin 0/33554432 bytes")
	sender.waitFor(t, "Item succeeded: large.bin")
	receiver.waitFor(t, "Received large.bin (33554432 bytes)")

	destination := filepath.Join(receiver.homeDir, "Downloads", "large.bin")
	if fileSHA256(t, sourcePath) != fileSHA256(t, destination) {
		t.Fatal("large streamed destination digest does not match the source")
	}
}

func TestSourceDigestChangeProducesAnExactPartialCompletionAndContinues(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	folder := filepath.Join(t.TempDir(), "partial-batch")
	if err := os.Mkdir(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(folder, "a-first.bin")
	changed := filepath.Join(folder, "b-changed.txt")
	last := filepath.Join(folder, "c-last.txt")
	if err := os.WriteFile(first, make([]byte, 2*1024*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(changed, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(last, []byte("independent success"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Stat(changed)
	if err != nil {
		t.Fatal(err)
	}

	sender.send(t, "send "+folder)
	receiver.waitFor(t, "Transfer Offer: 1 top-level roots, 3 files")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Receiving partial-batch/a-first.bin: 0/2097152 bytes")
	if err := os.WriteFile(changed, []byte("AFTER!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(changed, originalInfo.ModTime(), originalInfo.ModTime()); err != nil {
		t.Fatal(err)
	}

	sender.waitFor(t, "Transfer failed for partial-batch/b-changed.txt: source changed after approval")
	sender.waitFor(t, "Transfer Offer partially completed: 2 of 3 files succeeded; 1 failed.")
	receiver.waitFor(t, "Transfer Offer partially completed: 2 of 3 files succeeded; 1 failed.")

	destination := filepath.Join(receiver.homeDir, "Downloads", "partial-batch")
	assertFileContent(t, filepath.Join(destination, "a-first.bin"), string(make([]byte, 2*1024*1024)))
	assertFileContent(t, filepath.Join(destination, "c-last.txt"), "independent success")
	if _, err := os.Stat(filepath.Join(destination, "b-changed.txt")); !os.IsNotExist(err) {
		t.Fatalf("changed source was published as valid: %v", err)
	}
}

func TestSourceThatVanishesAfterApprovalFailsWithoutStoppingOtherFiles(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	folder := filepath.Join(t.TempDir(), "vanished-source")
	if err := os.Mkdir(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(folder, "a-first.bin")
	vanished := filepath.Join(folder, "b-vanished.txt")
	last := filepath.Join(folder, "c-last.txt")
	if err := os.WriteFile(first, make([]byte, 2*1024*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(vanished, []byte("vanish after approval"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(last, []byte("still succeeds"), 0o600); err != nil {
		t.Fatal(err)
	}

	sender.send(t, "send "+folder)
	receiver.waitFor(t, "Transfer Offer: 1 top-level roots, 3 files")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Receiving vanished-source/a-first.bin: 0/2097152 bytes")
	if err := os.Remove(vanished); err != nil {
		t.Fatal(err)
	}

	sender.waitFor(t, "Transfer failed for vanished-source/b-vanished.txt: source could not be opened")
	sender.waitFor(t, "Transfer Offer partially completed: 2 of 3 files succeeded; 1 failed.")
	receiver.waitFor(t, "Transfer Offer partially completed: 2 of 3 files succeeded; 1 failed.")
	destination := filepath.Join(receiver.homeDir, "Downloads", "vanished-source")
	assertFileContent(t, filepath.Join(destination, "c-last.txt"), "still succeeds")
	if _, err := os.Stat(filepath.Join(destination, "b-vanished.txt")); !os.IsNotExist(err) {
		t.Fatalf("vanished source was published: %v", err)
	}
}

func TestDestinationWriteFailurePreservesCompletedFilesAndContinues(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	folder := filepath.Join(t.TempDir(), "write-failure")
	if err := os.Mkdir(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	fixtures := map[string][]byte{
		"a-first.txt": []byte("first survives"),
		"b-race.bin":  make([]byte, 2*1024*1024),
		"c-last.txt":  []byte("last survives"),
	}
	for name, content := range fixtures {
		if err := os.WriteFile(filepath.Join(folder, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	sender.send(t, "send "+folder)
	receiver.waitFor(t, "Transfer Offer: 1 top-level roots, 3 files")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Receiving write-failure/b-race.bin: 0/2097152 bytes")
	destination := filepath.Join(receiver.homeDir, "Downloads", "write-failure")
	if err := os.WriteFile(filepath.Join(destination, "b-race.bin"), []byte("appeared after approval"), 0o600); err != nil {
		t.Fatal(err)
	}

	sender.waitFor(t, "Transfer failed for write-failure/b-race.bin: destination write failed")
	sender.waitFor(t, "Transfer Offer partially completed: 2 of 3 files succeeded; 1 failed.")
	receiver.waitFor(t, "Transfer Offer partially completed: 2 of 3 files succeeded; 1 failed.")
	assertFileContent(t, filepath.Join(destination, "a-first.txt"), "first survives")
	assertFileContent(t, filepath.Join(destination, "b-race.bin"), "appeared after approval")
	assertFileContent(t, filepath.Join(destination, "c-last.txt"), "last survives")
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

func TestMissingAndUnsupportedSourcesAreReportedWithoutEndingTheSession(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sender.send(t, "send")
	sender.waitFor(t, "Usage: send <path>...")
	missing := filepath.Join(t.TempDir(), "missing.txt")
	sender.send(t, "send "+missing)
	sender.waitFor(t, "Cannot create Transfer Offer")

	folder := filepath.Join(t.TempDir(), "batch")
	if err := os.Mkdir(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	valid := filepath.Join(folder, "valid.txt")
	if err := os.WriteFile(valid, []byte("still connected"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target-folder")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "must-not-transfer.txt"), []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(folder, "linked-folder")
	linked := createReparsePoint(link, target) == nil
	unreadablePath := filepath.Join(folder, "unreadable.txt")
	if err := os.WriteFile(unreadablePath, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	restoreReadable, unreadable := makeUnreadable(unreadablePath)
	t.Cleanup(restoreReadable)

	sender.send(t, "send "+folder)
	expectedFiles, expectedBytes := 2, 22
	if unreadable {
		expectedFiles, expectedBytes = 1, 15
	}
	receiver.waitFor(t, fmt.Sprintf("Transfer Offer: 1 top-level roots, %d files, 1 folders (%d bytes)", expectedFiles, expectedBytes))
	omissions := 0
	if linked {
		omissions++
	}
	if unreadable {
		omissions++
	}
	if omissions > 0 {
		receiver.waitFor(t, fmt.Sprintf("Omissions: %d unsupported, unreadable, or vanished entries", omissions))
		receiver.send(t, "details")
		if linked {
			receiver.waitFor(t, "omitted batch/linked-folder: symbolic link, junction, or reparse point is unsupported")
		}
		if unreadable {
			receiver.waitFor(t, "omitted batch/unreadable.txt:")
		}
	}
	receiver.send(t, "reject")
	sender.waitFor(t, "Peer rejected Transfer Offer batch")

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
	sender.waitFor(t, "Current: empty.dat 0/0 bytes")
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

func TestExistingFolderIsNotMergedAndConflictNameIsPreviewed(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	sourceRoot := t.TempDir()
	sourceFolder := filepath.Join(sourceRoot, "photos")
	if err := os.Mkdir(sourceFolder, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceFolder, "new.jpg"), []byte("new image"), 0o600); err != nil {
		t.Fatal(err)
	}
	downloads := filepath.Join(receiver.homeDir, "Downloads")
	existingFolder := filepath.Join(downloads, "PHOTOS")
	if err := os.MkdirAll(existingFolder, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(existingFolder, "existing.jpg"), []byte("existing image"), 0o600); err != nil {
		t.Fatal(err)
	}

	sender.send(t, "send "+sourceFolder)
	generatedFolder := filepath.Join(downloads, "photos (1)")
	receiver.waitFor(t, "Final path: "+generatedFolder)
	receiver.send(t, "accept")
	receiver.waitFor(t, "Received photos/new.jpg")

	assertFileContent(t, filepath.Join(existingFolder, "existing.jpg"), "existing image")
	if _, err := os.Stat(filepath.Join(existingFolder, "new.jpg")); !os.IsNotExist(err) {
		t.Fatalf("accepted folder silently merged into existing folder: %v", err)
	}
	assertFileContent(t, filepath.Join(generatedFolder, "new.jpg"), "new image")
}

func TestExecutableAndScriptFilesAreWarnedBeforeApproval(t *testing.T) {
	sender := startPeer(t, transferlyExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	script := filepath.Join(t.TempDir(), "install.PS1")
	if err := os.WriteFile(script, []byte("Write-Host safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+script)
	receiver.waitFor(t, "WARNING: 1 executable or script file(s) in this Transfer Offer")
	receiver.send(t, "details")
	receiver.waitFor(t, "file install.PS1 [EXECUTABLE OR SCRIPT]")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Received install.PS1")
	assertFileContent(t, filepath.Join(receiver.homeDir, "Downloads", "install.PS1"), "Write-Host safe")
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

func TestEitherPeerCanCancelTheActiveOfferAndQueuedWorkContinues(t *testing.T) {
	for _, cancelingPeer := range []string{"sender", "receiver"} {
		t.Run(cancelingPeer, func(t *testing.T) {
			sender := startPeer(t, slowIdleExecutable)
			receiver := startPeer(t, transferlyExecutable)
			verifyPeers(t, sender, receiver)

			directory := t.TempDir()
			active := filepath.Join(directory, "cancel-me.bin")
			queued := filepath.Join(directory, "keep-queued.txt")
			if err := os.WriteFile(active, make([]byte, 2*1024*1024), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(queued, []byte("queued work remains"), 0o600); err != nil {
				t.Fatal(err)
			}

			sender.send(t, "send "+active)
			receiver.waitFor(t, "Transfer Offer: cancel-me.bin")
			sender.send(t, "send "+queued)
			sender.waitFor(t, "Transfer Offer queued: keep-queued.txt")
			receiver.send(t, "accept")
			receiver.waitFor(t, "Receiving cancel-me.bin: 0/2097152 bytes")
			if cancelingPeer == "sender" {
				sender.send(t, "cancel")
			} else {
				receiver.send(t, "cancel")
			}
			sender.waitFor(t, "Transfer Offer canceled")
			receiver.waitFor(t, "Transfer Offer canceled")
			receiver.waitFor(t, "Transfer Offer: keep-queued.txt")
			receiver.send(t, "accept")
			receiver.waitFor(t, "Received keep-queued.txt")
			sender.waitFor(t, "Transfer complete: keep-queued.txt")

			downloads := filepath.Join(receiver.homeDir, "Downloads")
			if _, err := os.Stat(filepath.Join(downloads, "cancel-me.bin")); !os.IsNotExist(err) {
				t.Fatalf("canceled file was published: %v", err)
			}
			if _, err := os.Stat(filepath.Join(downloads, ".transferly-staging")); !os.IsNotExist(err) {
				t.Fatalf("canceled temporary content was retained: %v", err)
			}
			assertFileContent(t, filepath.Join(downloads, "keep-queued.txt"), "queued work remains")
		})
	}
}

func TestNetworkLossCleansIncompleteDataAndRetainsVerifiedFiles(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiver := startPeer(t, transferlyExecutable)
	verifyPeers(t, sender, receiver)

	folder := filepath.Join(t.TempDir(), "lost-connection")
	if err := os.Mkdir(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "a-complete.txt"), []byte("verified before loss"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "b-incomplete.bin"), make([]byte, 4*1024*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+folder)
	receiver.waitFor(t, "Transfer Offer: 1 top-level roots, 2 files")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Received lost-connection/a-complete.txt")
	receiver.waitFor(t, "Receiving lost-connection/b-incomplete.bin: 0/4194304 bytes")
	sender.kill(t)
	receiver.waitFor(t, "Transfer Session ended")

	destination := filepath.Join(receiver.homeDir, "Downloads", "lost-connection")
	assertFileContent(t, filepath.Join(destination, "a-complete.txt"), "verified before loss")
	if _, err := os.Stat(filepath.Join(destination, "b-incomplete.bin")); !os.IsNotExist(err) {
		t.Fatalf("incomplete file was published after network loss: %v", err)
	}
	if _, err := os.Stat(filepath.Join(receiver.homeDir, "Downloads", ".transferly-staging")); !os.IsNotExist(err) {
		t.Fatalf("incomplete staging survived network loss: %v", err)
	}

	replacement := startPeer(t, transferlyExecutable)
	verifyPeers(t, replacement, receiver)
}

func TestCrashStagingIsDetectedAndSafelyCleanedBeforeAFreshSessionRestarts(t *testing.T) {
	sender := startPeer(t, slowIdleExecutable)
	receiverHome := t.TempDir()
	receiver := startPeerWithHome(t, transferlyExecutable, receiverHome)
	verifyPeers(t, sender, receiver)

	sourcePath := filepath.Join(t.TempDir(), "interrupted.bin")
	if err := os.WriteFile(sourcePath, make([]byte, 4*1024*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+sourcePath)
	receiver.waitFor(t, "Transfer Offer: interrupted.bin")
	receiver.send(t, "accept")
	receiver.waitFor(t, "Receiving interrupted.bin: 0/4194304 bytes")
	receiver.kill(t)
	sender.waitFor(t, "Transfer Session ended")

	staging := filepath.Join(receiverHome, "Downloads", ".transferly-staging")
	if entries, err := os.ReadDir(staging); err != nil || len(entries) == 0 {
		t.Fatalf("abrupt process death did not leave detectable staging: entries=%v err=%v", entries, err)
	}

	replacement := startPeerWithHome(t, transferlyExecutable, receiverHome)
	verifyPeers(t, sender, replacement)
	fresh := filepath.Join(t.TempDir(), "fresh-restart.txt")
	if err := os.WriteFile(fresh, []byte("fresh verified session"), 0o600); err != nil {
		t.Fatal(err)
	}
	sender.send(t, "send "+fresh)
	replacement.waitFor(t, "Stale Transferly staging data detected")
	replacement.send(t, "accept")
	replacement.waitFor(t, "stale Transferly staging data must be removed")
	replacement.send(t, "cleanup-staging")
	replacement.waitFor(t, "Stale Transferly staging data removed. It was not used as resume state.")
	replacement.send(t, "accept")
	replacement.waitFor(t, "Received fresh-restart.txt")
	assertFileContent(t, filepath.Join(receiverHome, "Downloads", "fresh-restart.txt"), "fresh verified session")
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
	first.waitFor(t, "Incompatible wire protocol: local 3.0, Peer 1.0")
	second.waitFor(t, "Incompatible wire protocol: local 1.0, Peer 3.0")

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

func TestRealMDNSDiscoveryConnectsByAvailablePeerNumber(t *testing.T) {
	if os.Getenv("TRANSFERLY_TEST_MDNS") != "1" {
		t.Skip("set TRANSFERLY_TEST_MDNS=1 on a multicast-capable local network")
	}
	first := startPeerAt(t, transferlyExecutable, "0.0.0.0:0")
	second := startPeerAt(t, transferlyExecutable, "0.0.0.0:0")

	listing := first.waitFor(t, " at "+second.endpoint+" (untrusted discovery label)")
	selectionPattern := regexp.MustCompile(`\[(\d+)\][^\n]* at ` + regexp.QuoteMeta(second.endpoint))
	match := selectionPattern.FindStringSubmatch(listing)
	if match == nil {
		t.Fatalf("could not select discovered endpoint %s from output:\n%s", second.endpoint, listing)
	}

	first.send(t, "connect "+match[1])
	first.waitFor(t, "Discovery names do not establish identity or trust")
	firstCode := first.waitForCode(t, 0)
	secondCode := second.waitForCode(t, 0)
	if firstCode != secondCode {
		t.Fatalf("Peers displayed different verification codes: %s and %s", firstCode, secondCode)
	}
	first.send(t, "yes")
	second.send(t, "yes")
	first.waitFor(t, "Transfer Session verified")
	second.waitFor(t, "Transfer Session verified")
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

func openHostileSession(t *testing.T, receiver *peerProcess) *session.Session {
	t.Helper()
	previousCodes := receiver.matchCount(verificationCodePattern)
	connection, err := net.Dial("tcp4", receiver.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	opened := make(chan *session.Session, 1)
	openErrors := make(chan error, 1)
	codes := make(chan string, 1)
	go func() {
		secured, openError := session.Open(context.Background(), connection, session.Outbound, session.Version{Major: 3}, func(_ context.Context, code string) (bool, error) {
			codes <- code
			return true, nil
		})
		if openError != nil {
			openErrors <- openError
			return
		}
		opened <- secured
	}()
	receiverCode := receiver.waitForCode(t, previousCodes)
	var hostileCode string
	select {
	case hostileCode = <-codes:
	case err := <-openErrors:
		t.Fatalf("open hostile session: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for hostile Peer verification code")
	}
	if receiverCode != hostileCode {
		t.Fatalf("hostile Peer verification codes differ: %s and %s", receiverCode, hostileCode)
	}
	receiver.send(t, "yes")
	select {
	case secured := <-opened:
		t.Cleanup(func() { _ = secured.Close() })
		return secured
	case err := <-openErrors:
		t.Fatalf("open hostile session: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out opening hostile Transfer Session")
	}
	return nil
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
	endpointPattern         = regexp.MustCompile(`Endpoint: ((\d{1,3}\.){3}\d{1,3}:\d+)`)
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
	return startPeerAt(t, executable, "127.0.0.1:0")
}

func startPeerWithHome(t *testing.T, executable, homeDir string) *peerProcess {
	t.Helper()
	return startPeerAtWithHome(t, executable, "127.0.0.1:0", homeDir)
}

func startPeerAt(t *testing.T, executable, listenAddress string) *peerProcess {
	t.Helper()
	return startPeerAtWithHome(t, executable, listenAddress, t.TempDir())
}

func startPeerAtWithHome(t *testing.T, executable, listenAddress, homeDir string) *peerProcess {
	t.Helper()
	return startPeerWithArguments(t, executable, homeDir, "--listen", listenAddress)
}

func startPeerWithArguments(t *testing.T, executable, homeDir string, arguments ...string) *peerProcess {
	t.Helper()
	workDir := t.TempDir()
	command := exec.Command(executable, arguments...)
	command.Dir = workDir
	command.Env = append(os.Environ(), "HOME="+homeDir, "USERPROFILE="+homeDir, "GOMEMLIMIT=24MiB")
	if coverageEnabled() {
		// Each Peer process writes its own profile fragment; the fragments are
		// merged after the suite finishes.
		command.Env = append(command.Env, "GOCOVERDIR="+coverageDirectory)
	}
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
	endpointLine := peer.waitFor(t, "Endpoint: ")
	match := endpointPattern.FindStringSubmatch(endpointLine)
	if match == nil {
		t.Fatalf("could not parse endpoint from output:\n%s", peer.snapshot())
	}
	peer.endpoint = match[1]
	return peer
}

func assertNoProcessFiles(t *testing.T, peer *peerProcess) {
	t.Helper()
	for _, directory := range []string{peer.homeDir, peer.workDir} {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("hostile traffic created filesystem content in %s: %v", directory, entries)
		}
	}
}

func assertFileContent(t *testing.T, path, expected string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != expected {
		t.Fatalf("%s = %q, want %q", path, content, expected)
	}
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

func (p *peerProcess) kill(t *testing.T) {
	t.Helper()
	if p.command.Process == nil || p.command.ProcessState != nil {
		return
	}
	if err := p.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := p.command.Wait(); err == nil {
		t.Fatal("abruptly killed Transferly exited successfully")
	}
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
