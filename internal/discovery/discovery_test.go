package discovery

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestManagerAdvertisesAndTracksAvailablePeersWithoutTrustingNames(t *testing.T) {
	network := newFakeMulticast()
	manager := NewManager(network, Advertisement{
		ComputerName: "LOCAL-LAPTOP",
		Port:         53144,
		IPv4:         []net.IP{net.ParseIP("192.168.1.10")},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.Start(ctx)

	advertisement := network.waitForAdvertisement(t, 0)
	if advertisement.ComputerName != "LOCAL-LAPTOP" || advertisement.Port != 53144 || len(advertisement.IPv4) != 1 || !advertisement.IPv4[0].Equal(net.ParseIP("192.168.1.10")) {
		t.Fatalf("advertisement = %#v", advertisement)
	}

	network.events <- Event{ID: "self", Peer: Peer{ComputerName: "SPOOFED-SELF", IPv4: net.ParseIP("192.168.1.10"), Port: 53144}}
	network.events <- Event{ID: "terminal-injection", Peer: Peer{ComputerName: "\x1b[31mFAKE", IPv4: net.ParseIP("192.168.1.19"), Port: 50000}}
	network.events <- Event{ID: "first", Peer: Peer{ComputerName: "SHARED-NAME", IPv4: net.ParseIP("192.168.1.20"), Port: 50001}}
	network.events <- Event{ID: "second", Peer: Peer{ComputerName: "SHARED-NAME", IPv4: net.ParseIP("192.168.1.21"), Port: 50002}}

	peers := waitForPeers(t, manager, 2)
	if peers[0].ComputerName != "SHARED-NAME" || peers[0].Endpoint() != "192.168.1.20:50001" {
		t.Fatalf("first Available Peer = %#v", peers[0])
	}
	if peers[1].ComputerName != "SHARED-NAME" || peers[1].Endpoint() != "192.168.1.21:50002" {
		t.Fatalf("second Available Peer = %#v", peers[1])
	}

	network.events <- Event{ID: "first", Peer: peers[0], Lost: true}
	peers = waitForPeers(t, manager, 1)
	if peers[0].Endpoint() != "192.168.1.21:50002" {
		t.Fatalf("remaining Available Peer = %#v", peers[0])
	}
}

func TestManagerWithdrawsWhileBusyAndReadvertisesWithoutStoppingBrowsing(t *testing.T) {
	network := newFakeMulticast()
	manager := NewManager(network, Advertisement{
		ComputerName: "LOCAL-LAPTOP",
		Port:         53144,
		IPv4:         []net.IP{net.ParseIP("10.0.0.10")},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.Start(ctx)
	first := network.waitForRegistration(t, 0)

	manager.SetAvailable(false)
	first.waitForClose(t)

	network.events <- Event{ID: "other", Peer: Peer{ComputerName: "OTHER", IPv4: net.ParseIP("10.0.0.11"), Port: 53145}}
	waitForPeers(t, manager, 1)

	manager.SetAvailable(true)
	network.waitForAdvertisement(t, 1)
}

func TestManagerReportsMulticastFailuresWithoutEndingItsLifecycle(t *testing.T) {
	network := newFakeMulticast()
	network.advertiseError = errors.New("multicast unavailable")
	manager := NewManager(network, Advertisement{
		ComputerName: "LOCAL-LAPTOP",
		Port:         53144,
		IPv4:         []net.IP{net.ParseIP("10.0.0.10")},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.Start(ctx)

	select {
	case err := <-manager.Errors():
		if err == nil || err.Error() != "advertise Available Peer: multicast unavailable" {
			t.Fatalf("discovery error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for non-fatal discovery error")
	}

	network.events <- Event{ID: "other", Peer: Peer{ComputerName: "OTHER", IPv4: net.ParseIP("10.0.0.11"), Port: 53145}}
	waitForPeers(t, manager, 1)
}

func waitForPeers(t *testing.T, manager *Manager, count int) []Peer {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		peers := manager.Peers()
		if len(peers) == count {
			return peers
		}
		select {
		case <-manager.Changes():
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d Available Peers; got %#v", count, peers)
		}
	}
}

type fakeMulticast struct {
	mu             sync.Mutex
	events         chan Event
	advertisements []Advertisement
	registrations  []*fakeRegistration
	advertiseError error
	changed        chan struct{}
}

func newFakeMulticast() *fakeMulticast {
	return &fakeMulticast{events: make(chan Event, 16), changed: make(chan struct{}, 16)}
}

func (f *fakeMulticast) Browse(ctx context.Context, events chan<- Event) error {
	for {
		select {
		case event := <-f.events:
			events <- event
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *fakeMulticast) Advertise(advertisement Advertisement) (Registration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advertisements = append(f.advertisements, advertisement)
	if f.advertiseError != nil {
		f.signalChanged()
		return nil, f.advertiseError
	}
	registration := &fakeRegistration{closed: make(chan struct{})}
	f.registrations = append(f.registrations, registration)
	f.signalChanged()
	return registration, nil
}

func (f *fakeMulticast) waitForAdvertisement(t *testing.T, index int) Advertisement {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		f.mu.Lock()
		if len(f.advertisements) > index {
			advertisement := f.advertisements[index]
			f.mu.Unlock()
			return advertisement
		}
		f.mu.Unlock()
		select {
		case <-f.changed:
		case <-deadline.C:
			t.Fatalf("timed out waiting for advertisement %d", index)
		}
	}
}

func (f *fakeMulticast) waitForRegistration(t *testing.T, index int) *fakeRegistration {
	t.Helper()
	f.waitForAdvertisement(t, index)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.registrations) <= index {
		t.Fatalf("registration %d was not created", index)
	}
	return f.registrations[index]
}

func (f *fakeMulticast) signalChanged() {
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

type fakeRegistration struct {
	once   sync.Once
	closed chan struct{}
}

func (r *fakeRegistration) Close() {
	r.once.Do(func() { close(r.closed) })
}

func (r *fakeRegistration) waitForClose(t *testing.T) {
	t.Helper()
	select {
	case <-r.closed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for advertisement withdrawal")
	}
}
