// Package terminal runs Transferly's foreground interactive shell.
package terminal

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
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
	ListenAddress      string
	Version            session.Version
	ProductVersion     string
	ComputerName       string
	DefaultDestination string
	Discovery          discovery.Multicast                             // Replaceable mDNS/DNS-SD boundary.
	PeerDial           func(context.Context, string) (net.Conn, error) // Explicit Peer connection boundary.
	IdleWarningAfter   time.Duration
	IdleTimeoutAfter   time.Duration
	Faults             FaultConfig // Empty and inert unless built with -tags transferly_faults.
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
	if err := config.Faults.validate(); err != nil {
		return nil, err
	}
	if config.PeerDial == nil {
		config.PeerDial = func(ctx context.Context, endpoint string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: connectTimeout}
			return dialer.DialContext(ctx, "tcp4", endpoint)
		}
	}
	if err := validateListenAddress(config.ListenAddress); err != nil {
		return nil, err
	}
	if config.ProductVersion == "" {
		config.ProductVersion = "dev"
	}
	if config.ComputerName == "" {
		computerName, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("read Windows computer name: %w", err)
		}
		config.ComputerName = computerName
	}
	if err := validateComputerName(config.ComputerName); err != nil {
		return nil, err
	}
	if config.DefaultDestination == "" {
		destination, err := defaultDestination()
		if err != nil {
			return nil, fmt.Errorf("resolve Downloads destination: %w", err)
		}
		config.DefaultDestination = destination
	} else {
		destination, err := filepath.Abs(config.DefaultDestination)
		if err != nil {
			return nil, fmt.Errorf("resolve default incoming destination: %w", err)
		}
		config.DefaultDestination = filepath.Clean(destination)
	}
	listener, err := net.Listen("tcp4", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w. Check that the address is available and allow Transferly through Windows Firewall for the intended network profile; Transferly never requests elevation or changes firewall policy", config.ListenAddress, err)
	}
	clock := newSessionClock(config.Faults)
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
	a.line("Transferly %s (wire protocol %s)", a.config.ProductVersion, a.config.Version)
	a.line("Peer name: %s", a.config.ComputerName)
	a.line("Default incoming destination: %s (this run only)", a.config.DefaultDestination)
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
