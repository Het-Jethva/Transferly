// Package discovery finds Available Peers without assigning identity or trust.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// Advertisement is the complete information Transferly publishes through
// mDNS/DNS-SD. It intentionally cannot carry identity, session, or offer data.
type Advertisement struct {
	ComputerName string
	Port         int
	IPv4         []net.IP
}

// Peer is an untrusted discovery label and a directly reachable IPv4 endpoint.
type Peer struct {
	ComputerName string
	IPv4         net.IP
	Port         int
}

// Endpoint returns the numeric address accepted by the connect command.
func (p Peer) Endpoint() string {
	return net.JoinHostPort(p.IPv4.String(), strconv.Itoa(p.Port))
}

// Event reports that one discovered DNS-SD instance appeared or disappeared.
type Event struct {
	ID   string
	Peer Peer
	Lost bool
	Err  error
}

// Registration controls one foreground availability advertisement.
type Registration interface {
	Close()
}

// Multicast is the replaceable network boundary for mDNS/DNS-SD.
type Multicast interface {
	Browse(context.Context, chan<- Event) error
	Advertise(Advertisement) (Registration, error)
}

// Manager maintains a current, ordered list of Available Peers.
type Manager struct {
	multicast     Multicast
	advertisement Advertisement
	self          map[string]struct{}

	mu           sync.RWMutex
	peers        map[string]Peer
	context      context.Context
	registration Registration
	available    bool
	started      bool

	availabilityMu sync.Mutex
	changes        chan struct{}
	errors         chan error
}

// NewManager creates a process-lifetime manager. Call Start when the foreground
// terminal begins running.
func NewManager(multicast Multicast, advertisement Advertisement) *Manager {
	self := make(map[string]struct{}, len(advertisement.IPv4))
	for _, ip := range advertisement.IPv4 {
		if ipv4 := ip.To4(); ipv4 != nil {
			self[net.JoinHostPort(ipv4.String(), strconv.Itoa(advertisement.Port))] = struct{}{}
		}
	}
	return &Manager{
		multicast:     multicast,
		advertisement: cloneAdvertisement(advertisement),
		self:          self,
		peers:         make(map[string]Peer),
		changes:       make(chan struct{}, 1),
		errors:        make(chan error, 8),
	}
}

// Start begins browsing and advertises the idle foreground Peer. Discovery
// failures are reported through Errors and never stop the caller.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return
	}
	m.started = true
	m.context = ctx
	m.mu.Unlock()

	events := make(chan Event, 32)
	go m.consume(ctx, events)
	go func() {
		if err := m.multicast.Browse(ctx, events); err != nil && !errors.Is(err, context.Canceled) {
			m.report(fmt.Errorf("browse for Available Peers: %w", err))
		}
	}()
	m.SetAvailable(true)
	go func() {
		<-ctx.Done()
		m.SetAvailable(false)
	}()
}

// SetAvailable publishes availability while idle and withdraws it while a
// Transfer Session is pending or active.
func (m *Manager) SetAvailable(available bool) {
	m.availabilityMu.Lock()
	defer m.availabilityMu.Unlock()

	m.mu.Lock()
	if !m.started || m.available == available {
		m.mu.Unlock()
		return
	}
	m.available = available
	registration := m.registration
	m.registration = nil
	ctx := m.context
	m.mu.Unlock()

	if registration != nil {
		registration.Close()
	}
	if !available || ctx == nil || ctx.Err() != nil {
		return
	}

	created, err := m.multicast.Advertise(cloneAdvertisement(m.advertisement))
	if err != nil {
		m.report(fmt.Errorf("advertise Available Peer: %w", err))
		return
	}
	m.mu.Lock()
	if !m.available || m.context != ctx || ctx.Err() != nil {
		m.mu.Unlock()
		created.Close()
		return
	}
	m.registration = created
	m.mu.Unlock()
}

// Peers returns a stable snapshot sorted by untrusted name, address, and port.
func (m *Manager) Peers() []Peer {
	m.mu.RLock()
	peers := make([]Peer, 0, len(m.peers))
	for _, peer := range m.peers {
		peer.IPv4 = append(net.IP(nil), peer.IPv4...)
		peers = append(peers, peer)
	}
	m.mu.RUnlock()
	sort.Slice(peers, func(first, second int) bool {
		firstName := strings.ToLower(peers[first].ComputerName)
		secondName := strings.ToLower(peers[second].ComputerName)
		if firstName != secondName {
			return firstName < secondName
		}
		return peers[first].Endpoint() < peers[second].Endpoint()
	})
	return peers
}

// Changes is notified whenever the Available Peer snapshot changes.
func (m *Manager) Changes() <-chan struct{} {
	return m.changes
}

// Errors reports non-fatal multicast failures.
func (m *Manager) Errors() <-chan error {
	return m.errors
}

func (m *Manager) consume(ctx context.Context, events <-chan Event) {
	for {
		select {
		case event := <-events:
			if event.Err != nil {
				m.report(event.Err)
				continue
			}
			m.apply(event)
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) apply(event Event) {
	if event.ID == "" {
		return
	}
	if event.Lost {
		m.mu.Lock()
		_, existed := m.peers[event.ID]
		delete(m.peers, event.ID)
		m.mu.Unlock()
		if existed {
			m.changed()
		}
		return
	}

	ipv4 := event.Peer.IPv4.To4()
	computerName, safeName := safeComputerName(event.Peer.ComputerName)
	if ipv4 == nil || ipv4.IsUnspecified() || ipv4.IsMulticast() || event.Peer.Port < 1 || event.Peer.Port > 65535 || !safeName {
		return
	}
	peer := Peer{ComputerName: computerName, IPv4: append(net.IP(nil), ipv4...), Port: event.Peer.Port}
	if _, isSelf := m.self[peer.Endpoint()]; isSelf {
		return
	}

	m.mu.Lock()
	previous, existed := m.peers[event.ID]
	m.peers[event.ID] = peer
	m.mu.Unlock()
	if !existed || previous.ComputerName != peer.ComputerName || previous.Endpoint() != peer.Endpoint() {
		m.changed()
	}
}

func (m *Manager) changed() {
	select {
	case m.changes <- struct{}{}:
	default:
	}
}

func (m *Manager) report(err error) {
	select {
	case m.errors <- err:
	default:
	}
}

func safeComputerName(candidate string) (string, bool) {
	name := strings.TrimSpace(candidate)
	if name == "" || len([]byte(name)) > 200 || !utf8.ValidString(name) {
		return "", false
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return "", false
		}
	}
	return name, true
}

func cloneAdvertisement(advertisement Advertisement) Advertisement {
	clone := advertisement
	clone.IPv4 = make([]net.IP, 0, len(advertisement.IPv4))
	for _, ip := range advertisement.IPv4 {
		clone.IPv4 = append(clone.IPv4, append(net.IP(nil), ip...))
	}
	return clone
}
