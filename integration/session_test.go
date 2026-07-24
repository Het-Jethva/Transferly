package integration_test

import (
	"bufio"
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

	if err := buildExecutable(transferlyExecutable); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildExecutable(incompatibleTransferlyExecutable, "-ldflags", "-X main.wireMajor=2"); err != nil {
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
	first.waitFor(t, "Incompatible wire protocol: local 1.0, Peer 2.0")
	second.waitFor(t, "Incompatible wire protocol: local 2.0, Peer 1.0")

	if first.contains("Verification code:") || second.contains("Verification code:") {
		t.Fatal("an incompatible connection reached human verification")
	}
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

	mu     sync.Mutex
	output strings.Builder
	change chan struct{}
}

func startPeer(t *testing.T, executable string) *peerProcess {
	t.Helper()
	workDir := t.TempDir()
	command := exec.Command(executable, "--listen", "127.0.0.1:0")
	command.Dir = workDir
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = command.Stdout

	peer := &peerProcess{command: command, stdin: stdin, workDir: workDir, change: make(chan struct{}, 1)}
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
