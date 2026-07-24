package discovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grandcat/zeroconf"
)

const (
	serviceType       = "_transferly._tcp"
	serviceDomain     = "local."
	computerNameField = "name="
	advertisementTTL  = 8
	browseDuration    = 1200 * time.Millisecond
	browsePause       = 800 * time.Millisecond
	staleAfter        = 6 * time.Second
)

// MDNS is Transferly's IPv4 mDNS/DNS-SD network boundary.
type MDNS struct{}

// NewMDNS creates the production multicast boundary.
func NewMDNS() *MDNS {
	return &MDNS{}
}

// Advertise publishes only the dynamic IPv4 endpoint and Windows computer
// name. The DNS instance and host labels are derived from those same values.
func (m *MDNS) Advertise(advertisement Advertisement) (Registration, error) {
	name, safeName := safeComputerName(advertisement.ComputerName)
	if !safeName {
		return nil, errors.New("computer name is not safe to advertise")
	}
	if advertisement.Port < 1 || advertisement.Port > 65535 {
		return nil, errors.New("advertised port must be between 1 and 65535")
	}

	addresses := make([]string, 0, len(advertisement.IPv4))
	seen := make(map[string]struct{}, len(advertisement.IPv4))
	for _, candidate := range advertisement.IPv4 {
		ipv4 := candidate.To4()
		if ipv4 == nil || ipv4.IsUnspecified() || ipv4.IsMulticast() || ipv4.IsLoopback() {
			continue
		}
		address := ipv4.String()
		if _, exists := seen[address]; !exists {
			seen[address] = struct{}{}
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return nil, errors.New("no non-loopback IPv4 endpoint is available to advertise")
	}
	sort.Strings(addresses)

	labelMaterial := name + "\x00" + strconv.Itoa(advertisement.Port) + "\x00" + strings.Join(addresses, ",")
	digest := sha256.Sum256([]byte(labelMaterial))
	suffix := hex.EncodeToString(digest[:8])
	instance := "Transferly-" + suffix + "-" + strconv.Itoa(advertisement.Port)
	host := "transferly-" + suffix + ".local."
	server, err := zeroconf.RegisterProxy(instance, serviceType, serviceDomain, advertisement.Port, host, addresses, []string{computerNameField + name}, nil)
	if err != nil {
		return nil, err
	}
	server.TTL(advertisementTTL)
	return serverRegistration{server: server}, nil
}

// Browse continuously refreshes a short lease for each DNS-SD instance so
// withdrawn, crashed, and stale advertisements disappear from the terminal.
func (m *MDNS) Browse(ctx context.Context, events chan<- Event) error {
	type sighting struct {
		peer     Peer
		lastSeen time.Time
	}
	known := make(map[string]sighting)
	lastError := ""

	for {
		observed, err := scanMDNS(ctx)
		now := time.Now()
		if err != nil && !errors.Is(err, context.Canceled) {
			if err.Error() != lastError {
				if !sendEvent(ctx, events, Event{Err: fmt.Errorf("mDNS browse unavailable; manual connect remains available: %w", err)}) {
					return ctx.Err()
				}
				lastError = err.Error()
			}
		} else {
			lastError = ""
			for id, peer := range observed {
				previous, existed := known[id]
				known[id] = sighting{peer: peer, lastSeen: now}
				if !existed || previous.peer.ComputerName != peer.ComputerName || previous.peer.Endpoint() != peer.Endpoint() {
					if !sendEvent(ctx, events, Event{ID: id, Peer: peer}) {
						return ctx.Err()
					}
				}
			}
		}

		for id, previous := range known {
			if now.Sub(previous.lastSeen) < staleAfter {
				continue
			}
			delete(known, id)
			if !sendEvent(ctx, events, Event{ID: id, Peer: previous.peer, Lost: true}) {
				return ctx.Err()
			}
		}

		timer := time.NewTimer(browsePause)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
	}
}

func scanMDNS(ctx context.Context) (map[string]Peer, error) {
	resolver, err := zeroconf.NewResolver(zeroconf.SelectIPTraffic(zeroconf.IPv4))
	if err != nil {
		return nil, err
	}
	scanContext, cancel := context.WithTimeout(ctx, browseDuration)
	defer cancel()
	entries := make(chan *zeroconf.ServiceEntry, 32)
	if err := resolver.Browse(scanContext, serviceType, serviceDomain, entries); err != nil {
		return nil, err
	}

	observed := make(map[string]Peer)
	for {
		select {
		case entry, open := <-entries:
			if !open {
				return observed, nil
			}
			name, ok := advertisedComputerName(entry.Text)
			if !ok || entry.Port < 1 || entry.Port > 65535 {
				continue
			}
			for _, candidate := range entry.AddrIPv4 {
				ipv4 := candidate.To4()
				if ipv4 == nil || ipv4.IsUnspecified() || ipv4.IsMulticast() {
					continue
				}
				peer := Peer{ComputerName: name, IPv4: append(net.IP(nil), ipv4...), Port: entry.Port}
				id := entry.Instance + "|" + peer.Endpoint()
				observed[id] = peer
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-scanContext.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return observed, nil
		}
	}
}

func advertisedComputerName(text []string) (string, bool) {
	name := ""
	for _, field := range text {
		if !strings.HasPrefix(field, computerNameField) {
			continue
		}
		if name != "" {
			return "", false
		}
		name = strings.TrimSpace(strings.TrimPrefix(field, computerNameField))
	}
	return safeComputerName(name)
}

func sendEvent(ctx context.Context, destination chan<- Event, event Event) bool {
	select {
	case destination <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

type serverRegistration struct {
	server *zeroconf.Server
}

func (r serverRegistration) Close() {
	if r.server != nil {
		r.server.Shutdown()
	}
}
