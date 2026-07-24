// Package terminal runs Transferly's foreground interactive shell.
package terminal

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Het-Jethva/Transferly/internal/session"
)

const connectTimeout = 4 * time.Second

// Config contains process-lifetime settings. It is intentionally not persisted.
type Config struct {
	ListenAddress string
	Version       session.Version
}

// App owns one foreground listener and at most one Transfer Session.
type App struct {
	config   Config
	listener net.Listener
	output   io.Writer

	rootContext context.Context
	cancelRoot  context.CancelFunc

	mu      sync.Mutex
	current *attempt
	closed  bool
	writeMu sync.Mutex
}

type attempt struct {
	context  context.Context
	cancel   context.CancelFunc
	remote   string
	answer   chan bool
	raw      net.Conn
	secure   *session.Session
	waiting  bool
	stopping bool
}

// New starts the IPv4 listener. Call Run to print endpoints and accept input.
func New(config Config, output io.Writer) (*App, error) {
	if output == nil {
		return nil, errors.New("terminal output is required")
	}
	if config.ListenAddress == "" {
		config.ListenAddress = "0.0.0.0:0"
	}
	if err := validateListenAddress(config.ListenAddress); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp4", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", config.ListenAddress, err)
	}
	rootContext, cancelRoot := context.WithCancel(context.Background())
	return &App{
		config:      config,
		listener:    listener,
		output:      output,
		rootContext: rootContext,
		cancelRoot:  cancelRoot,
	}, nil
}

// Run serves incoming Peers and handles commands until quit, end-of-input, or
// an interrupt. It does not create files or retain state after returning.
func (a *App) Run(input io.Reader) error {
	a.line("Transferly wire protocol %s", a.config.Version)
	for _, endpoint := range advertisedEndpoints(a.listener.Addr()) {
		a.line("Endpoint: %s", endpoint)
	}
	a.line("Commands: connect <IPv4:port>, disconnect, quit")

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

	if a.answerPendingConfirmation(line) {
		return false
	}

	fields := strings.Fields(line)
	switch strings.ToLower(fields[0]) {
	case "connect":
		if len(fields) != 2 {
			a.line("Usage: connect <IPv4:port>")
			return false
		}
		a.connect(fields[1])
	case "disconnect":
		if len(fields) != 1 {
			a.line("Usage: disconnect")
			return false
		}
		a.disconnect()
	case "quit", "exit":
		return true
	case "help":
		a.line("Commands: connect <IPv4:port>, disconnect, quit")
	default:
		a.line("Unknown command %q. Type help for available commands.", fields[0])
	}
	return false
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
			_ = connection.Close()
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
	defer a.mu.Unlock()
	if a.closed || a.current != nil {
		return nil, false
	}
	attemptContext, cancel := context.WithCancel(a.rootContext)
	current := &attempt{
		context: attemptContext,
		cancel:  cancel,
		remote:  remote,
		answer:  make(chan bool, 1),
	}
	a.current = current
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
	current.waiting = false
	established = true
	a.mu.Unlock()
	a.line("Transfer Session verified with %s.", current.remote)

	waitError := secured.Wait()
	a.mu.Lock()
	wasCurrent := a.current == current
	if wasCurrent {
		a.current = nil
	}
	a.mu.Unlock()
	current.cancel()
	_ = secured.Close()
	if wasCurrent {
		if waitError != nil {
			a.line("Transfer Session ended after a connection error: %v", waitError)
		} else {
			a.line("Transfer Session ended.")
		}
	}
}

func (a *App) reportSessionError(remote string, err error) {
	var versionError *session.VersionError
	switch {
	case errors.Is(err, session.ErrLocalRejected):
		a.line("Verification did not match; connection closed.")
	case errors.Is(err, session.ErrPeerRejected):
		a.line("Peer rejected verification; connection closed.")
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
	if a.current == current {
		a.current = nil
	}
	current.waiting = false
	a.mu.Unlock()
	current.cancel()
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

func advertisedEndpoints(address net.Addr) []string {
	tcpAddress, ok := address.(*net.TCPAddr)
	if !ok {
		return []string{address.String()}
	}
	if !tcpAddress.IP.IsUnspecified() {
		return []string{net.JoinHostPort(tcpAddress.IP.String(), strconv.Itoa(tcpAddress.Port))}
	}

	seen := make(map[string]struct{})
	var endpoints []string
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, networkInterface := range interfaces {
			if networkInterface.Flags&net.FlagUp == 0 || networkInterface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addresses, addressError := networkInterface.Addrs()
			if addressError != nil {
				continue
			}
			for _, candidate := range addresses {
				ip, _, parseError := net.ParseCIDR(candidate.String())
				if parseError != nil || ip.To4() == nil || ip.IsUnspecified() || ip.IsLoopback() {
					continue
				}
				endpoint := net.JoinHostPort(ip.String(), strconv.Itoa(tcpAddress.Port))
				if _, exists := seen[endpoint]; !exists {
					seen[endpoint] = struct{}{}
					endpoints = append(endpoints, endpoint)
				}
			}
		}
	}
	if len(endpoints) == 0 {
		endpoints = append(endpoints, net.JoinHostPort("127.0.0.1", strconv.Itoa(tcpAddress.Port)))
	}
	sort.Strings(endpoints)
	return endpoints
}
