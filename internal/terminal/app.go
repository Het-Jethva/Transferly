// Package terminal runs Transferly's foreground interactive shell.
package terminal

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Het-Jethva/Transferly/internal/discovery"
	"github.com/Het-Jethva/Transferly/internal/session"
)

const (
	connectTimeout     = 4 * time.Second
	defaultIdleWarning = 14 * time.Minute
	defaultIdleTimeout = 15 * time.Minute
	maxQueuedOffers    = 64
	messageActivity    = "activity"
	messageKeepAlive   = "keepalive"
)

// Config contains process-lifetime settings. It is intentionally not persisted.
type Config struct {
	ListenAddress    string
	Version          session.Version
	ComputerName     string
	Discovery        discovery.Multicast // Replaceable mDNS/DNS-SD boundary.
	CorruptDigest    bool                // Used only by protocol-boundary integration builds.
	IdleWarningAfter time.Duration
	IdleTimeoutAfter time.Duration
	StreamChunkDelay time.Duration // Nonzero only in process-level idle tests.
	ControllableTime bool          // Enabled only in process-level idle tests.
}

// App owns one foreground listener and at most one Transfer Session.
type App struct {
	config    Config
	listener  net.Listener
	output    io.Writer
	clock     sessionClock
	discovery *discovery.Manager

	rootContext context.Context
	cancelRoot  context.CancelFunc

	mu      sync.Mutex
	current *attempt
	closed  bool
	writeMu sync.Mutex
}

type attempt struct {
	context           context.Context
	cancel            context.CancelFunc
	remote            string
	answer            chan bool
	raw               net.Conn
	secure            *session.Session
	role              session.Role
	waiting           bool
	stopping          bool
	incoming          *incomingOffer
	outgoing          *outgoingOffer
	outgoingQueue     []*outgoingOffer
	coordinatorQueue  []queuedOffer
	coordinatorActive bool
	lastActivity      time.Time
	idleWake          chan struct{}
	idleWarned        bool
	transferActive    bool
	finished          chan struct{}
	finishOnce        sync.Once
}

type queuedOffer struct {
	incoming *incomingOffer
	outgoing *outgoingOffer
}

type incomingOffer struct {
	id          string
	name        string
	size        int64
	destination string
	finalPath   string
	actions     chan offerAction
	waiting     bool
	accepted    bool
	stagingPath string
	stagingFile *os.File
	reviewDone  chan struct{}
}

type outgoingOffer struct {
	id        string
	path      string
	name      string
	size      int64
	modified  time.Time
	decision  chan session.Message
	result    chan session.Message
	queued    chan bool
	submitted bool
}

type offerAction struct {
	kind        string
	destination string
}

// New starts the IPv4 listener. Call Run to print endpoints and accept input.
func New(config Config, output io.Writer) (*App, error) {
	if output == nil {
		return nil, errors.New("terminal output is required")
	}
	if config.ListenAddress == "" {
		config.ListenAddress = "0.0.0.0:0"
	}
	if config.IdleWarningAfter == 0 {
		config.IdleWarningAfter = defaultIdleWarning
	}
	if config.IdleTimeoutAfter == 0 {
		config.IdleTimeoutAfter = defaultIdleTimeout
	}
	if config.IdleWarningAfter <= 0 || config.IdleTimeoutAfter <= config.IdleWarningAfter {
		return nil, errors.New("idle timeout must be greater than the positive idle warning duration")
	}
	if config.StreamChunkDelay < 0 {
		return nil, errors.New("stream chunk delay cannot be negative")
	}
	if err := validateListenAddress(config.ListenAddress); err != nil {
		return nil, err
	}
	if config.ComputerName == "" {
		computerName, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("read Windows computer name: %w", err)
		}
		config.ComputerName = computerName
	}
	listener, err := net.Listen("tcp4", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", config.ListenAddress, err)
	}
	clock := sessionClock(realSessionClock{})
	if config.ControllableTime {
		clock = newManualSessionClock()
	}
	rootContext, cancelRoot := context.WithCancel(context.Background())
	application := &App{
		config:      config,
		listener:    listener,
		output:      output,
		clock:       clock,
		rootContext: rootContext,
		cancelRoot:  cancelRoot,
	}
	discoveryAddresses := discoverableIPv4(listener.Addr(), config.Discovery != nil)
	if len(discoveryAddresses) > 0 {
		multicast := config.Discovery
		if multicast == nil {
			multicast = discovery.NewMDNS()
		}
		application.discovery = discovery.NewManager(multicast, discovery.Advertisement{
			ComputerName: config.ComputerName,
			Port:         listener.Addr().(*net.TCPAddr).Port,
			IPv4:         discoveryAddresses,
		})
	}
	return application, nil
}

// Run serves incoming Peers and handles commands until quit, end-of-input, or
// an interrupt. It does not retain identity, trust, history, or configuration.
func (a *App) Run(input io.Reader) error {
	a.line("Transferly wire protocol %s", a.config.Version)
	for _, endpoint := range advertisedEndpoints(a.listener.Addr()) {
		a.line("Endpoint: %s", endpoint)
	}
	a.printCommands()
	if a.discovery != nil {
		a.line("Discovery: mDNS/DNS-SD is searching the local IPv4 network. Computer names are untrusted hints.")
		a.showAvailablePeers()
		a.discovery.Start(a.rootContext)
		go a.observeDiscovery()
	} else {
		a.line("Discovery unavailable for this listener; manual connect <IPv4:port> remains available.")
	}

	go a.acceptConnections()

	lines := make(chan string)
	scanErrors := make(chan error, 1)
	go scanInput(input, lines, scanErrors)

	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupts)

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				a.shutdown()
				return <-scanErrors
			}
			if a.handleLine(line) {
				a.shutdown()
				return nil
			}
		case <-interrupts:
			a.line("Interrupted; disconnecting and exiting.")
			a.shutdown()
			return nil
		case <-a.rootContext.Done():
			return nil
		}
	}
}

func scanInput(input io.Reader, lines chan<- string, result chan<- error) {
	defer close(lines)
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
	result <- scanner.Err()
}

func (a *App) handleLine(line string) bool {
	line = strings.TrimSpace(line)
	if line == "" {
		return false
	}
	if a.advanceControllableTime(line) {
		return false
	}
	a.noteTerminalActivity()
	if a.answerPendingConfirmation(line) || a.answerPendingOffer(line) {
		return false
	}

	command, argument := splitCommand(line)
	switch command {
	case "connect":
		if argument == "" || strings.ContainsAny(argument, " \t") {
			a.line("Usage: connect <peer-number|IPv4:port>")
			return false
		}
		a.connectTarget(argument)
	case "send":
		path, ok := onePathArgument(argument)
		if !ok {
			a.line("Usage: send <path>")
			return false
		}
		a.sendFile(path)
	case "disconnect":
		if argument != "" {
			a.line("Usage: disconnect")
			return false
		}
		a.disconnect()
	case "keep-alive":
		if argument != "" {
			a.line("Usage: keep-alive")
			return false
		}
		a.keepAlive()
	case "quit", "exit":
		return true
	case "help":
		a.printCommands()
	case "accept", "reject", "destination":
		a.line("There is no Transfer Offer awaiting approval.")
	default:
		a.line("Unknown command %q. Type help for available commands.", command)
	}
	return false
}

func splitCommand(line string) (string, string) {
	for index, character := range line {
		if character == ' ' || character == '\t' {
			return strings.ToLower(line[:index]), strings.TrimSpace(line[index+1:])
		}
	}
	return strings.ToLower(line), ""
}

func onePathArgument(argument string) (string, bool) {
	argument = strings.TrimSpace(argument)
	if len(argument) >= 2 && argument[0] == '"' && argument[len(argument)-1] == '"' {
		argument = argument[1 : len(argument)-1]
	} else if strings.ContainsAny(argument, "\"\r\n") {
		return "", false
	}
	return argument, argument != ""
}

func (a *App) printCommands() {
	a.line("Commands: connect <peer-number|IPv4:port>, send <path>, keep-alive, disconnect, quit")
}

func (a *App) connectTarget(target string) {
	if number, err := strconv.Atoi(target); err == nil {
		peers := []discovery.Peer(nil)
		if a.discovery != nil {
			peers = a.discovery.Peers()
		}
		if number < 1 || number > len(peers) {
			a.line("Available Peer %d is not currently listed. Use an IPv4:port endpoint instead.", number)
			return
		}
		peer := peers[number-1]
		a.line("Connecting to Available Peer %d at %s. Discovery names do not establish identity or trust.", number, peer.Endpoint())
		a.connect(peer.Endpoint())
		return
	}
	a.connect(target)
}

func (a *App) observeDiscovery() {
	for {
		select {
		case <-a.discovery.Changes():
			a.showAvailablePeers()
		case err := <-a.discovery.Errors():
			a.line("Discovery warning: %v", err)
		case <-a.rootContext.Done():
			return
		}
	}
}

func (a *App) showAvailablePeers() {
	peers := a.discovery.Peers()
	if len(peers) == 0 {
		a.line("No Available Peers discovered; use connect <IPv4:port> if multicast is unavailable.")
		return
	}
	a.line("Available Peers:")
	for index, peer := range peers {
		a.line("  [%d] %s at %s (untrusted discovery label)", index+1, peer.ComputerName, peer.Endpoint())
	}
}

func (a *App) setDiscoveryAvailable(available bool) {
	if a.discovery != nil {
		a.discovery.SetAvailable(available)
	}
}

func (a *App) answerPendingConfirmation(line string) bool {
	a.mu.Lock()
	current := a.current
	if current == nil || !current.waiting {
		a.mu.Unlock()
		return false
	}

	var answer bool
	switch strings.ToLower(line) {
	case "yes", "y":
		answer = true
	case "no", "n":
		answer = false
	default:
		a.mu.Unlock()
		a.line("Please type yes if the codes match, or no to close the connection.")
		return true
	}
	current.waiting = false
	answerChannel := current.answer
	a.mu.Unlock()

	select {
	case answerChannel <- answer:
	case <-current.context.Done():
	}
	return true
}

func (a *App) answerPendingOffer(line string) bool {
	a.mu.Lock()
	current := a.current
	if current == nil || current.incoming == nil || !current.incoming.waiting {
		a.mu.Unlock()
		return false
	}
	incoming := current.incoming
	a.mu.Unlock()

	command, argument := splitCommand(line)
	action := offerAction{}
	switch command {
	case "accept":
		if argument != "" {
			a.line("Usage: accept")
			return true
		}
		action.kind = "accept"
	case "reject":
		if argument != "" {
			a.line("Usage: reject")
			return true
		}
		action.kind = "reject"
	case "destination":
		path, ok := onePathArgument(argument)
		if !ok {
			a.line("Usage: destination <path>")
			return true
		}
		action.kind = "destination"
		action.destination = path
	case "send", "help", "keep-alive", "disconnect", "quit", "exit":
		return false
	default:
		a.line("Choose accept, reject, or destination <path> for this Transfer Offer.")
		return true
	}

	select {
	case incoming.actions <- action:
	case <-current.context.Done():
	}
	return true
}

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
		dialer := net.Dialer{Timeout: connectTimeout}
		connection, err := dialer.DialContext(current.context, "tcp4", endpoint)
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
		case "result":
			if !a.routeOutgoing(current, message, false) {
				return errors.New("received a result for an unknown Transfer Offer")
			}
		case "abort":
			a.abortIncoming(current, message)
		case messageActivity:
			if a.config.ControllableTime {
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

func (a *App) sendFile(path string) {
	a.mu.Lock()
	current := a.current
	if current == nil || current.secure == nil {
		a.mu.Unlock()
		a.line("Open and verify a Transfer Session before using send.")
		return
	}
	a.mu.Unlock()

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		a.line("Cannot offer %q: %v", path, err)
		return
	}
	info, err := os.Lstat(absolutePath)
	if err != nil {
		a.line("Cannot offer %q: %v", path, err)
		return
	}
	if !info.Mode().IsRegular() {
		a.line("Cannot offer %q: only one readable regular file is supported.", path)
		return
	}
	probe, err := os.Open(absolutePath)
	if err != nil {
		a.line("Cannot offer %q: file is not readable: %v", path, err)
		return
	}
	_ = probe.Close()
	id, err := newOfferID()
	if err != nil {
		a.line("Cannot create Transfer Offer: %v", err)
		return
	}
	outgoing := &outgoingOffer{
		id:       id,
		path:     absolutePath,
		name:     info.Name(),
		size:     info.Size(),
		modified: info.ModTime(),
		decision: make(chan session.Message, 1),
		result:   make(chan session.Message, 1),
		queued:   make(chan bool, 1),
	}

	a.mu.Lock()
	if a.current != current || current.secure == nil {
		a.mu.Unlock()
		a.line("The Transfer Session changed before the offer could be sent; try again.")
		return
	}
	if current.role == session.Inbound {
		if len(current.coordinatorQueue) >= maxQueuedOffers {
			a.mu.Unlock()
			a.line("Transfer Offer queue is full; wait for an offer to finish before sending %s.", outgoing.name)
			return
		}
		queued := current.coordinatorActive || len(current.coordinatorQueue) > 0
		current.coordinatorQueue = append(current.coordinatorQueue, queuedOffer{outgoing: outgoing})
		next := a.takeCoordinatorOfferLocked(current)
		a.mu.Unlock()
		if queued {
			a.line("Transfer Offer queued: %s (%d bytes).", outgoing.name, outgoing.size)
		}
		a.startCoordinatorOffer(current, next)
		return
	}
	if current.outgoing != nil {
		if len(current.outgoingQueue) >= maxQueuedOffers {
			a.mu.Unlock()
			a.line("Transfer Offer queue is full; wait for an offer to finish before sending %s.", outgoing.name)
			return
		}
		outgoing.submitted = true
		current.outgoingQueue = append(current.outgoingQueue, outgoing)
		secured := current.secure
		a.mu.Unlock()
		if err := secured.Send(offerMessage(outgoing)); err != nil {
			a.line("Could not queue Transfer Offer %s: %v", outgoing.name, err)
			_ = secured.Close()
			return
		}
		select {
		case queued := <-outgoing.queued:
			if queued {
				a.line("Transfer Offer queued: %s (%d bytes).", outgoing.name, outgoing.size)
			} else {
				a.line("Peer could not queue Transfer Offer %s.", outgoing.name)
			}
		case <-current.context.Done():
		}
		return
	}
	outgoing.submitted = true
	current.outgoing = outgoing
	secured := current.secure
	a.mu.Unlock()
	if err := secured.Send(offerMessage(outgoing)); err != nil {
		a.failOutgoing(current, outgoing, "Could not send Transfer Offer: %v", err)
		_ = secured.Close()
		return
	}
	go a.runOutgoing(current, outgoing)
}

func offerMessage(outgoing *outgoingOffer) session.Message {
	return session.Message{Type: "offer", OfferID: outgoing.id, Name: outgoing.name, Size: outgoing.size}
}

func (a *App) runOutgoing(current *attempt, outgoing *outgoingOffer) {
	if !outgoing.submitted {
		if err := current.secure.Send(offerMessage(outgoing)); err != nil {
			a.failOutgoing(current, outgoing, "Could not send Transfer Offer: %v", err)
			return
		}
		outgoing.submitted = true
	}
	a.line("Transfer Offer sent: %s (%d bytes). Waiting for the Peer.", outgoing.name, outgoing.size)

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
		a.line("Peer rejected Transfer Offer %s. No file content was sent.", outgoing.name)
		return
	}

	a.startTransfer(current)
	source, err := os.Open(outgoing.path)
	if err != nil {
		_ = current.secure.Send(session.Message{Type: "abort", OfferID: outgoing.id, Reason: "source could not be opened"})
		a.failOutgoing(current, outgoing, "Transfer failed for %s: source could not be opened: %v", outgoing.name, err)
		return
	}
	before, err := source.Stat()
	if err != nil || before.Size() != outgoing.size || !before.ModTime().Equal(outgoing.modified) {
		_ = source.Close()
		_ = current.secure.Send(session.Message{Type: "abort", OfferID: outgoing.id, Reason: "source changed after approval"})
		a.failOutgoing(current, outgoing, "Transfer failed for %s: source changed after the offer was created.", outgoing.name)
		return
	}

	progress := a.progress("Sending "+outgoing.name, outgoing.size)
	var sourceReader io.Reader = source
	if a.config.StreamChunkDelay > 0 {
		sourceReader = &delayedReader{context: current.context, source: source, delay: a.config.StreamChunkDelay}
	}
	_, streamError := current.secure.SendStream(current.context, session.Message{
		Type: "content", OfferID: outgoing.id, Size: outgoing.size,
	}, sourceReader, outgoing.size, progress, func(digest string) (session.Message, error) {
		after, err := source.Stat()
		if err != nil || after.Size() != outgoing.size || !after.ModTime().Equal(outgoing.modified) {
			return session.Message{}, errors.New("source changed while it was being read")
		}
		if a.config.CorruptDigest {
			digest = corruptSHA256(digest)
		}
		return session.Message{Type: "complete", OfferID: outgoing.id, Size: outgoing.size, Digest: digest}, nil
	})
	closeError := source.Close()
	if streamError != nil || closeError != nil {
		a.failOutgoing(current, outgoing, "Transfer failed for %s because the source changed or could not be read completely.", outgoing.name)
		_ = current.secure.Close()
		return
	}

	var result session.Message
	select {
	case result = <-outgoing.result:
	case <-current.context.Done():
		return
	}
	a.clearOutgoing(current, outgoing)
	if result.Success == nil || !*result.Success {
		reason := result.Reason
		if reason == "" {
			reason = "the Peer could not verify or publish it"
		}
		a.line("Transfer failed for %s: %s.", outgoing.name, reason)
		return
	}
	a.line("Transfer complete: %s (%d bytes).", outgoing.name, outgoing.size)
}

type delayedReader struct {
	context context.Context
	source  io.Reader
	delay   time.Duration
}

func (r *delayedReader) Read(destination []byte) (int, error) {
	timer := time.NewTimer(r.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return r.source.Read(destination)
	case <-r.context.Done():
		return 0, r.context.Err()
	}
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

func (a *App) handleIncomingOffer(current *attempt, message session.Message) error {
	if message.OfferID == "" || message.Size < 0 || !safeFileName(message.Name) {
		return errors.New("Peer sent an invalid Transfer Offer")
	}
	destination, err := defaultDestination()
	if err != nil {
		return fmt.Errorf("resolve Downloads destination: %w", err)
	}
	finalPath, err := resolveFinalPath(destination, message.Name)
	if err != nil {
		return fmt.Errorf("resolve offered destination: %w", err)
	}
	incoming := &incomingOffer{
		id:          message.OfferID,
		name:        message.Name,
		size:        message.Size,
		destination: destination,
		finalPath:   finalPath,
		actions:     make(chan offerAction, 1),
		waiting:     true,
	}

	a.mu.Lock()
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
	return nil
}

func (a *App) reviewIncomingOffer(current *attempt, incoming *incomingOffer) error {
	a.showIncoming(incoming)
	for {
		select {
		case action := <-incoming.actions:
			switch action.kind {
			case "destination":
				destination, absoluteError := filepath.Abs(action.destination)
				if absoluteError != nil {
					a.line("Cannot use destination %q: %v", action.destination, absoluteError)
					continue
				}
				finalPath, resolveError := resolveFinalPath(destination, incoming.name)
				if resolveError != nil {
					a.line("Cannot use destination %q: %v", action.destination, resolveError)
					continue
				}
				incoming.destination = filepath.Clean(destination)
				incoming.finalPath = finalPath
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

func (a *App) showIncoming(incoming *incomingOffer) {
	a.line("Transfer Offer: %s (%d bytes)", incoming.name, incoming.size)
	a.line("Destination: %s", incoming.destination)
	a.line("Final path: %s", incoming.finalPath)
	a.line("Choose accept, reject, or destination <path>.")
}

func prepareIncoming(incoming *incomingOffer) error {
	if err := os.MkdirAll(incoming.destination, 0o755); err != nil {
		return err
	}
	stagingDirectory := filepath.Join(incoming.destination, ".transferly-staging")
	if err := os.MkdirAll(stagingDirectory, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(stagingDirectory, "incoming-*.part")
	if err != nil {
		return err
	}
	incoming.stagingPath = file.Name()
	incoming.stagingFile = file
	return nil
}

func (a *App) receiveContent(current *attempt, message session.Message) error {
	a.mu.Lock()
	incoming := current.incoming
	a.mu.Unlock()
	if incoming == nil || !incoming.accepted || incoming.id != message.OfferID || incoming.size != message.Size || incoming.stagingFile == nil {
		return errors.New("Peer sent file content without an accepted matching Transfer Offer")
	}

	progress := a.progress("Receiving "+incoming.name, incoming.size)
	digest, err := current.secure.ReceiveStream(current.context, incoming.stagingFile, incoming.size, progress)
	if err != nil {
		a.cleanupIncoming(current)
		return fmt.Errorf("receive %s: %w", incoming.name, err)
	}
	if err := incoming.stagingFile.Sync(); err != nil {
		a.cleanupIncoming(current)
		return fmt.Errorf("flush temporary file: %w", err)
	}
	if err := incoming.stagingFile.Close(); err != nil {
		incoming.stagingFile = nil
		a.cleanupIncoming(current)
		return fmt.Errorf("close temporary file: %w", err)
	}
	incoming.stagingFile = nil

	completion, err := current.secure.Receive()
	if err != nil {
		a.cleanupIncoming(current)
		return err
	}
	if completion.Type != "complete" || completion.OfferID != incoming.id || completion.Size != incoming.size {
		a.cleanupIncoming(current)
		return errors.New("Peer sent an invalid file completion frame")
	}
	if len(completion.Digest) != 64 || !strings.EqualFold(completion.Digest, digest) {
		a.removeIncomingFiles(incoming)
		success := false
		_ = current.secure.Send(session.Message{Type: "result", OfferID: incoming.id, Success: &success, Reason: "size or SHA-256 integrity check failed"})
		a.clearIncoming(current, incoming)
		a.line("Transfer failed for %s: size or SHA-256 integrity check failed; incomplete content was removed.", incoming.name)
		return nil
	}
	if err := publishWithoutOverwrite(incoming.stagingPath, incoming.finalPath); err != nil {
		a.removeIncomingFiles(incoming)
		success := false
		_ = current.secure.Send(session.Message{Type: "result", OfferID: incoming.id, Success: &success, Reason: "final path became unavailable"})
		a.clearIncoming(current, incoming)
		a.line("Transfer failed for %s: final path became unavailable; existing content was not overwritten.", incoming.name)
		return nil
	}
	incoming.stagingPath = ""
	removeEmptyStagingDirectory(incoming.destination)
	success := true
	if err := current.secure.Send(session.Message{Type: "result", OfferID: incoming.id, Success: &success}); err != nil {
		a.clearIncoming(current, incoming)
		return err
	}
	a.clearIncoming(current, incoming)
	a.line("Received %s (%d bytes) at %s.", incoming.name, incoming.size, incoming.finalPath)
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
	a.line("Transfer failed for %s: %s; incomplete content was removed.", incoming.name, reason)
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

func (a *App) cleanupIncoming(current *attempt) {
	a.mu.Lock()
	incoming := current.incoming
	current.incoming = nil
	var reviewDone <-chan struct{}
	if incoming != nil {
		reviewDone = incoming.reviewDone
	}
	a.mu.Unlock()
	if reviewDone != nil {
		<-reviewDone
	}
	if incoming != nil {
		a.removeIncomingFiles(incoming)
	}
}

func (a *App) removeIncomingFiles(incoming *incomingOffer) {
	if incoming.stagingFile != nil {
		_ = incoming.stagingFile.Close()
		incoming.stagingFile = nil
	}
	if incoming.stagingPath != "" {
		_ = os.Remove(incoming.stagingPath)
		incoming.stagingPath = ""
	}
	removeEmptyStagingDirectory(incoming.destination)
}

func removeEmptyStagingDirectory(destination string) {
	_ = os.Remove(filepath.Join(destination, ".transferly-staging"))
}

func defaultDestination() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Downloads"), nil
}

func resolveFinalPath(destination, name string) (string, error) {
	if !safeFileName(name) {
		return "", errors.New("file name is not safe")
	}
	destination, err := filepath.Abs(destination)
	if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(destination)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	existing := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		existing[strings.ToLower(entry.Name())] = struct{}{}
	}
	candidate := name
	extension := filepath.Ext(name)
	stem := strings.TrimSuffix(name, extension)
	for suffix := 1; ; suffix++ {
		if _, found := existing[strings.ToLower(candidate)]; !found {
			return filepath.Join(destination, candidate), nil
		}
		candidate = fmt.Sprintf("%s (%d)%s", stem, suffix, extension)
	}
}

func safeFileName(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && !strings.ContainsAny(name, "/\\\x00\r\n")
}

func newOfferID() (string, error) {
	identifier := make([]byte, 16)
	if _, err := rand.Read(identifier); err != nil {
		return "", err
	}
	return hex.EncodeToString(identifier), nil
}

func corruptSHA256(digest string) string {
	if digest == "" {
		return digest
	}
	if digest[0] == '0' {
		return "1" + digest[1:]
	}
	return "0" + digest[1:]
}

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

func validatePeerEndpoint(endpoint string) error {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return errors.New("use the form IPv4:port, for example 192.168.1.20:53144")
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil || strings.Contains(host, ":") {
		return errors.New("host must be a numeric IPv4 address, not a computer name")
	}
	if ip.IsUnspecified() || ip.IsMulticast() {
		return errors.New("address must identify one reachable Peer")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	return nil
}

func validateListenAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: use IPv4:port", address)
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil || strings.Contains(host, ":") {
		return fmt.Errorf("invalid listen address %q: host must be numeric IPv4", address)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 {
		return fmt.Errorf("invalid listen address %q: port must be between 0 and 65535", address)
	}
	return nil
}

func discoverableIPv4(address net.Addr, includeLoopback bool) []net.IP {
	tcpAddress, ok := address.(*net.TCPAddr)
	if !ok {
		return nil
	}
	if !tcpAddress.IP.IsUnspecified() {
		ipv4 := tcpAddress.IP.To4()
		if ipv4 != nil && (!ipv4.IsLoopback() || includeLoopback) {
			return []net.IP{append(net.IP(nil), ipv4...)}
		}
		return nil
	}

	seen := make(map[string]struct{})
	var addresses []net.IP
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagUp == 0 || networkInterface.Flags&net.FlagLoopback != 0 {
			continue
		}
		interfaceAddresses, addressError := networkInterface.Addrs()
		if addressError != nil {
			continue
		}
		for _, candidate := range interfaceAddresses {
			ip, _, parseError := net.ParseCIDR(candidate.String())
			ipv4 := ip.To4()
			if parseError != nil || ipv4 == nil || ipv4.IsUnspecified() || ipv4.IsLoopback() {
				continue
			}
			if _, exists := seen[ipv4.String()]; !exists {
				seen[ipv4.String()] = struct{}{}
				addresses = append(addresses, append(net.IP(nil), ipv4...))
			}
		}
	}
	sort.Slice(addresses, func(first, second int) bool { return addresses[first].String() < addresses[second].String() })
	return addresses
}

func advertisedEndpoints(address net.Addr) []string {
	tcpAddress, ok := address.(*net.TCPAddr)
	if !ok {
		return []string{address.String()}
	}
	if !tcpAddress.IP.IsUnspecified() {
		return []string{net.JoinHostPort(tcpAddress.IP.String(), strconv.Itoa(tcpAddress.Port))}
	}

	addresses := discoverableIPv4(address, false)
	endpoints := make([]string, 0, len(addresses))
	for _, ip := range addresses {
		endpoints = append(endpoints, net.JoinHostPort(ip.String(), strconv.Itoa(tcpAddress.Port)))
	}
	if len(endpoints) == 0 {
		endpoints = append(endpoints, net.JoinHostPort("127.0.0.1", strconv.Itoa(tcpAddress.Port)))
	}
	sort.Strings(endpoints)
	return endpoints
}
