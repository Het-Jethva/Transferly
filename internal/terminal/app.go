// Package terminal runs Transferly's foreground interactive shell.
package terminal

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
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
	connectTimeout      = 4 * time.Second
	defaultIdleWarning  = 14 * time.Minute
	defaultIdleTimeout  = 15 * time.Minute
	maxQueuedOffers     = 64
	messageActivity     = "activity"
	messageKeepAlive    = "keepalive"
	reasonOfferCanceled = "Transfer Offer canceled"
	streamChunkBytes    = 1024 * 1024
)

var errOfferCanceled = errors.New(reasonOfferCanceled)

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
	pendingIncoming   map[string]*incomingOffer
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
	id                 string
	canceled           bool
	manifest           offerManifest
	destination        string
	actions            chan offerAction
	waiting            bool
	accepted           bool
	collecting         bool
	receivingFiles     map[string]*receivingFile
	finalPaths         map[string]string
	createdDirectories []string
	staleStaging       bool
	completedFile      int
	failedFile         int
	fileOutcomes       map[string]struct{}
	rootCount          int
	reviewDone         chan struct{}
}

type receivingFile struct {
	entry        *manifestEntry
	stagingPath  string
	file         *os.File
	destination  *recoverableWriter
	digest       hash.Hash
	received     int64
	progress     session.Progress
	writeFailure error
}

type outgoingOffer struct {
	id        string
	canceled  bool
	manifest  offerManifest
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
			if a.cancelActiveOffer() {
				continue
			}
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
		paths, ok := parsePathArguments(argument)
		if !ok {
			a.line("Usage: send <path>...")
			return false
		}
		a.sendPaths(paths)
	case "cancel":
		if argument != "" {
			a.line("Usage: cancel")
			return false
		}
		if !a.cancelActiveOffer() {
			a.line("There is no active Transfer Offer to cancel.")
		}
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
	case "accept", "reject", "destination", "details", "cleanup-staging":
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
	a.line("Commands: connect <peer-number|IPv4:port>, send <path>..., cancel, keep-alive, disconnect, quit")
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
	case "details":
		if argument != "" {
			a.line("Usage: details")
			return true
		}
		action.kind = "details"
	case "cleanup-staging":
		if argument != "" {
			a.line("Usage: cleanup-staging")
			return true
		}
		action.kind = "cleanup-staging"
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
		a.line("Choose accept, reject, destination <path>, details, or cleanup-staging for this Transfer Offer.")
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
			var sourceReader io.Reader = source
			if a.config.StreamChunkDelay > 0 {
				sourceReader = &delayedReader{context: current.context, source: source, delay: a.config.StreamChunkDelay}
			}
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
	if a.config.CorruptDigest {
		completion.Digest = corruptSHA256(digest)
	}
	return current.secure.SendChecked(completion, func() error {
		a.mu.Lock()
		defer a.mu.Unlock()
		if outgoing.canceled {
			return errOfferCanceled
		}
		return nil
	})
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
	entryCount := int64(message.FileCount) + int64(message.FolderCount)
	if len(message.OfferID) != 32 || message.RootCount < 1 || message.RootCount > maxManifestEntries || message.FileCount < 0 || message.FolderCount < 0 || entryCount < 1 || entryCount > maxManifestEntries || message.TotalBytes < 0 {
		return errors.New("Peer sent an invalid or oversized Transfer Offer header")
	}
	if _, err := hex.DecodeString(message.OfferID); err != nil {
		return errors.New("Peer sent an invalid Transfer Offer identifier")
	}
	destination, err := defaultDestination()
	if err != nil {
		return fmt.Errorf("resolve Downloads destination: %w", err)
	}
	incoming := &incomingOffer{id: message.OfferID, destination: destination, actions: make(chan offerAction, 1), collecting: true, rootCount: message.RootCount}
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
		if entry.Kind == manifestFile {
			files++
			if entry.Size > incoming.manifest.TotalBytes-bytes {
				return errors.New("Peer sent a Transfer Offer whose byte total overflows or is inconsistent")
			}
			bytes += entry.Size
		} else if entry.Kind == manifestFolder {
			folders++
		} else {
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

func prepareIncoming(incoming *incomingOffer) error {
	if incoming.staleStaging {
		return errors.New("stale Transferly staging data must be removed with cleanup-staging or a different destination selected")
	}
	if err := rejectExistingReparseComponents(incoming.destination); err != nil {
		return fmt.Errorf("destination is unsafe: %w", err)
	}
	if err := os.MkdirAll(incoming.destination, 0o755); err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	if err := rejectExistingReparseComponents(incoming.destination); err != nil {
		return fmt.Errorf("destination is unsafe: %w", err)
	}
	info, err := os.Stat(incoming.destination)
	if err != nil || !info.IsDir() {
		return errors.New("destination is not a folder")
	}
	if available, reliable, err := availableDiskBytes(incoming.destination); err != nil {
		return fmt.Errorf("check available disk space: %w", err)
	} else if reliable && uint64(incoming.manifest.TotalBytes) > available {
		return fmt.Errorf("insufficient disk space: need %d bytes, only %d bytes are available", incoming.manifest.TotalBytes, available)
	}

	incoming.createdDirectories = nil
	rollbackRoots := func() { removeCreatedDirectories(incoming) }
	for _, root := range incoming.manifest.Roots {
		var rootEntry *manifestEntry
		for index := range incoming.manifest.Entries {
			if manifestPathKey(incoming.manifest.Entries[index].Path) == manifestPathKey(root) {
				rootEntry = &incoming.manifest.Entries[index]
				break
			}
		}
		if rootEntry == nil {
			rollbackRoots()
			return fmt.Errorf("manifest root %q has no entry", root)
		}
		finalPath := incoming.finalPaths[manifestPathKey(root)]
		if err := ensurePathBeneath(incoming.destination, finalPath); err != nil {
			rollbackRoots()
			return err
		}
		if _, err := os.Lstat(finalPath); err == nil || !os.IsNotExist(err) {
			rollbackRoots()
			return fmt.Errorf("final path %s became unavailable after approval", finalPath)
		}
		if rootEntry.Kind == manifestFolder {
			if err := os.Mkdir(finalPath, 0o755); err != nil {
				rollbackRoots()
				return fmt.Errorf("final path %s became unavailable after approval: %w", finalPath, err)
			}
			incoming.createdDirectories = append(incoming.createdDirectories, finalPath)
		}
	}
	folders := make([]manifestEntry, 0, incoming.manifest.FolderCount)
	for _, entry := range incoming.manifest.Entries {
		if entry.Kind == manifestFolder && strings.Contains(entry.Path, "/") {
			folders = append(folders, entry)
		}
	}
	sort.Slice(folders, func(i, j int) bool {
		return strings.Count(folders[i].Path, "/") < strings.Count(folders[j].Path, "/")
	})
	for _, entry := range folders {
		path := incomingPath(incoming, entry)
		if err := ensurePathBeneath(incoming.destination, path); err != nil {
			rollbackRoots()
			return err
		}
		if err := os.Mkdir(path, 0o755); err != nil {
			rollbackRoots()
			return fmt.Errorf("create manifest folder %s: %w", entry.Path, err)
		}
		incoming.createdDirectories = append(incoming.createdDirectories, path)
	}
	staging := filepath.Join(incoming.destination, ".transferly-staging")
	if err := ensurePathBeneath(incoming.destination, staging); err != nil {
		rollbackRoots()
		return err
	}
	if err := rejectReparsePoint(staging); err != nil {
		rollbackRoots()
		return fmt.Errorf("staging area is unsafe: %w", err)
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		rollbackRoots()
		return err
	}
	return nil
}

func incomingPath(incoming *incomingOffer, entry manifestEntry) string {
	parts := strings.Split(entry.Path, "/")
	root := incoming.finalPaths[manifestPathKey(parts[0])]
	if len(parts) == 1 {
		return root
	}
	return filepath.Join(root, filepath.Join(parts[1:]...))
}

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

type recoverableWriter struct {
	destination io.Writer
	failure     error
}

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
	a.removeIncomingStaging(incoming)
	removeCreatedDirectories(incoming)
}

func (a *App) removeIncomingStaging(incoming *incomingOffer) {
	for key, stream := range incoming.receivingFiles {
		if stream.file != nil {
			_ = stream.file.Close()
			stream.file = nil
		}
		if stream.stagingPath != "" {
			_ = os.Remove(stream.stagingPath)
			stream.stagingPath = ""
		}
		delete(incoming.receivingFiles, key)
	}
	removeEmptyStagingDirectory(incoming.destination)
}

func (a *App) removeIncomingStream(incoming *incomingOffer, path string) {
	key := manifestPathKey(path)
	stream := incoming.receivingFiles[key]
	if stream == nil {
		return
	}
	if stream.file != nil {
		_ = stream.file.Close()
	}
	if stream.stagingPath != "" {
		_ = os.Remove(stream.stagingPath)
	}
	delete(incoming.receivingFiles, key)
	removeEmptyStagingDirectory(incoming.destination)
}

func removeCreatedDirectories(incoming *incomingOffer) {
	for index := len(incoming.createdDirectories) - 1; index >= 0; index-- {
		_ = os.Remove(incoming.createdDirectories[index]) // Removes only empty paths; completed files are never touched.
	}
	incoming.createdDirectories = nil
}

func removeEmptyStagingDirectory(destination string) {
	_ = os.Remove(filepath.Join(destination, ".transferly-staging"))
}

func hasStaleStaging(destination string) (bool, error) {
	staging := filepath.Join(destination, ".transferly-staging")
	info, err := os.Lstat(staging)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("the Transferly staging path is not a safe folder")
	}
	if err := rejectReparsePoint(staging); err != nil {
		return false, err
	}
	entries, err := os.ReadDir(staging)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		_ = os.Remove(staging)
		return false, nil
	}
	return true, nil
}

func cleanupStaleStaging(destination string) error {
	staging := filepath.Join(destination, ".transferly-staging")
	if err := ensurePathBeneath(destination, staging); err != nil {
		return err
	}
	if err := rejectReparsePoint(staging); err != nil {
		return err
	}
	return os.RemoveAll(staging)
}

func defaultDestination() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Downloads"), nil
}

func resolveFinalPath(destination, name string) (string, error) {
	destination, reserved, err := destinationNameReservations(destination)
	if err != nil {
		return "", err
	}
	return resolveFinalPathWithReservations(destination, name, reserved)
}

func destinationNameReservations(destination string) (string, map[string]struct{}, error) {
	destination, err := filepath.Abs(destination)
	if err != nil {
		return "", nil, err
	}
	entries, err := os.ReadDir(destination)
	if err != nil && !os.IsNotExist(err) {
		return "", nil, err
	}
	reserved := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		reserved[manifestPathKey(entry.Name())] = struct{}{}
	}
	return destination, reserved, nil
}

func resolveFinalPathWithReservations(destination, name string, reserved map[string]struct{}) (string, error) {
	if err := validateManifestPath(name); err != nil || strings.Contains(name, "/") {
		if err == nil {
			err = errors.New("top-level name contains a path separator")
		}
		return "", err
	}
	candidate := name
	extension := filepath.Ext(name)
	stem := strings.TrimSuffix(name, extension)
	for suffix := 1; ; suffix++ {
		if err := validateWindowsComponent(candidate); err != nil {
			return "", err
		}
		resolved := filepath.Join(destination, candidate)
		if err := ensurePathBeneath(destination, resolved); err != nil {
			return "", err
		}
		key := manifestPathKey(candidate)
		if _, found := reserved[key]; !found {
			reserved[key] = struct{}{}
			return resolved, nil
		}
		candidate = fmt.Sprintf("%s (%d)%s", stem, suffix, extension)
	}
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
