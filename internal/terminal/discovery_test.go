package terminal_test

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Het-Jethva/Transferly/internal/discovery"
	"github.com/Het-Jethva/Transferly/internal/session"
	"github.com/Het-Jethva/Transferly/internal/terminal"
)

func TestAvailablePeerNumberUsesTheManualTransferSessionFlow(t *testing.T) {
	peerListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer peerListener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptError := peerListener.Accept()
		if acceptError == nil {
			accepted <- connection
		}
	}()

	multicast := newTerminalMulticast()
	output := newLockedOutput()
	application, err := terminal.New(terminal.Config{
		ListenAddress: "127.0.0.1:0",
		Version:       session.Version{Major: 2},
		ComputerName:  "LOCAL-LAPTOP",
		Discovery:     multicast,
	}, output)
	if err != nil {
		t.Fatal(err)
	}
	inputReader, inputWriter := io.Pipe()
	runDone := make(chan error, 1)
	go func() { runDone <- application.Run(inputReader) }()
	defer func() {
		_, _ = io.WriteString(inputWriter, "quit\n")
		_ = inputWriter.Close()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("terminal did not stop")
		}
	}()

	registration := multicast.waitForRegistration(t)
	endpoint := peerListener.Addr().String()
	host, portText, _ := net.SplitHostPort(endpoint)
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	multicast.events <- discovery.Event{
		ID:   "other",
		Peer: discovery.Peer{ComputerName: "UNTRUSTED-NAME", IPv4: net.ParseIP(host), Port: port},
	}
	output.waitFor(t, "[1] UNTRUSTED-NAME at "+endpoint+" (untrusted discovery label)")

	_, _ = io.WriteString(inputWriter, "connect 1\n")
	output.waitFor(t, "Connecting to Available Peer 1 at "+endpoint+". Discovery names do not establish identity or trust.")
	select {
	case connection := <-accepted:
		defer connection.Close()
	case <-time.After(time.Second):
		t.Fatal("connect by Available Peer number did not dial its IPv4 endpoint")
	}
	registration.waitForClose(t)
	_, _ = io.WriteString(inputWriter, "disconnect\n")
}

func TestMDNSFailureDoesNotPreventManualConnection(t *testing.T) {
	peerListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer peerListener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptError := peerListener.Accept()
		if acceptError == nil {
			accepted <- connection
		}
	}()

	multicast := newTerminalMulticast()
	multicast.advertiseError = errors.New("multicast blocked")
	output := newLockedOutput()
	application, err := terminal.New(terminal.Config{
		ListenAddress: "127.0.0.1:0",
		Version:       session.Version{Major: 2},
		ComputerName:  "LOCAL-LAPTOP",
		Discovery:     multicast,
	}, output)
	if err != nil {
		t.Fatal(err)
	}
	inputReader, inputWriter := io.Pipe()
	runDone := make(chan error, 1)
	go func() { runDone <- application.Run(inputReader) }()
	defer func() {
		_, _ = io.WriteString(inputWriter, "quit\n")
		_ = inputWriter.Close()
		<-runDone
	}()

	output.waitFor(t, "Discovery warning: advertise Available Peer: multicast blocked")
	_, _ = io.WriteString(inputWriter, "connect "+peerListener.Addr().String()+"\n")
	select {
	case connection := <-accepted:
		_ = connection.Close()
	case <-time.After(time.Second):
		t.Fatal("manual connection was unavailable after mDNS failure")
	}
}

func TestStaleAvailablePeerCannotBeSelected(t *testing.T) {
	multicast := newTerminalMulticast()
	output := newLockedOutput()
	application, err := terminal.New(terminal.Config{
		ListenAddress: "127.0.0.1:0",
		Version:       session.Version{Major: 2},
		ComputerName:  "LOCAL-LAPTOP",
		Discovery:     multicast,
	}, output)
	if err != nil {
		t.Fatal(err)
	}
	inputReader, inputWriter := io.Pipe()
	runDone := make(chan error, 1)
	go func() { runDone <- application.Run(inputReader) }()
	defer func() {
		_, _ = io.WriteString(inputWriter, "quit\n")
		_ = inputWriter.Close()
		<-runDone
	}()

	multicast.waitForRegistration(t)
	peer := discovery.Peer{ComputerName: "GONE", IPv4: net.ParseIP("127.0.0.2"), Port: 54321}
	multicast.events <- discovery.Event{ID: "gone", Peer: peer}
	output.waitFor(t, "[1] GONE at 127.0.0.2:54321")
	noPeersBefore := output.count("No Available Peers discovered")
	multicast.events <- discovery.Event{ID: "gone", Peer: peer, Lost: true}
	output.waitForCount(t, "No Available Peers discovered", noPeersBefore+1)

	_, _ = io.WriteString(inputWriter, "connect 1\n")
	output.waitFor(t, "Available Peer 1 is not currently listed. Use an IPv4:port endpoint instead.")
}

type terminalMulticast struct {
	events         chan discovery.Event
	registration   chan *terminalRegistration
	advertiseError error
}

func newTerminalMulticast() *terminalMulticast {
	return &terminalMulticast{events: make(chan discovery.Event, 8), registration: make(chan *terminalRegistration, 4)}
}

func (m *terminalMulticast) Browse(ctx context.Context, destination chan<- discovery.Event) error {
	for {
		select {
		case event := <-m.events:
			destination <- event
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *terminalMulticast) Advertise(discovery.Advertisement) (discovery.Registration, error) {
	if m.advertiseError != nil {
		return nil, m.advertiseError
	}
	registration := &terminalRegistration{closed: make(chan struct{})}
	m.registration <- registration
	return registration, nil
}

func (m *terminalMulticast) waitForRegistration(t *testing.T) *terminalRegistration {
	t.Helper()
	select {
	case registration := <-m.registration:
		return registration
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for discovery advertisement")
		return nil
	}
}

type terminalRegistration struct {
	once   sync.Once
	closed chan struct{}
}

func (r *terminalRegistration) Close() {
	r.once.Do(func() { close(r.closed) })
}

func (r *terminalRegistration) waitForClose(t *testing.T) {
	t.Helper()
	select {
	case <-r.closed:
	case <-time.After(time.Second):
		t.Fatal("Available Peer advertisement was not withdrawn")
	}
}

type lockedOutput struct {
	mu      sync.Mutex
	content strings.Builder
	changed chan struct{}
}

func newLockedOutput() *lockedOutput {
	return &lockedOutput{changed: make(chan struct{}, 1)}
}

func (o *lockedOutput) Write(bytes []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	written, err := o.content.Write(bytes)
	select {
	case o.changed <- struct{}{}:
	default:
	}
	return written, err
}

func (o *lockedOutput) snapshot() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.content.String()
}

func (o *lockedOutput) count(text string) int {
	return strings.Count(o.snapshot(), text)
}

func (o *lockedOutput) waitFor(t *testing.T, text string) {
	t.Helper()
	o.waitForCount(t, text, 1)
}

func (o *lockedOutput) waitForCount(t *testing.T, text string, count int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		content := o.snapshot()
		if strings.Count(content, text) >= count {
			return
		}
		select {
		case <-o.changed:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %q\noutput:\n%s", text, content)
		}
	}
}
