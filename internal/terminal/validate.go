package terminal

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

func validateComputerName(name string) error {
	if strings.TrimSpace(name) == "" || len([]byte(name)) > 200 || !utf8.ValidString(name) {
		return errors.New("computer name must contain 1 to 200 bytes of valid text")
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return errors.New("computer name cannot contain terminal control characters")
		}
	}
	return nil
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
