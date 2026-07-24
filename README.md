# Transferly

Transferly is a foreground terminal application for direct file exchange between reachable Peers. It discovers Available Peers with mDNS/DNS-SD when multicast works, retains manual IPv4 endpoints as a fallback, opens temporary human-verified Transfer Sessions, and can safely copy explicitly approved regular files in either direction.

Transferly uses no account, cloud service, relay, configuration file, persistent identity, remembered trust, telemetry, or log file.

## Requirements

- Windows 10 or 11 x64
- Go 1.22 or newer to build from source

## Build

```powershell
go build -o transferly.exe ./cmd/transferly
```

The resulting executable is portable and does not require a Go runtime on the destination computer.

## Use

Start `transferly.exe` in a foreground terminal on both computers. Windows Firewall may ask whether to permit inbound connections; allow the executable on the network profile you intend to use.

Each idle Peer advertises only its dynamic endpoint and Windows computer name and prints one or more endpoints:

```text
Endpoint: 192.168.1.20:53144
```

Available Peers on the local multicast-capable network appear as a numbered list:

```text
Available Peers:
  [1] LAPTOP-NAME at 192.168.1.20:53144 (untrusted discovery label)
```

Connect by current list number or by a manually supplied endpoint:

```text
connect 1
connect 192.168.1.20:53144
```

Computer names and discovery records are untrusted availability hints; they never establish identity or remembered trust. Stale advertisements disappear, duplicate names remain distinguishable by endpoint, and a Peer withdraws its advertisement while a connection is pending or active. If mDNS is blocked or unavailable, startup and manual endpoint connection continue normally.

Both terminals display a six-digit code. Compare the codes through an in-person or otherwise trusted channel, then type `yes` on both terminals only when they match. Type `no` on either terminal if they do not match.

In a verified Transfer Session, either Peer can offer readable regular files repeatedly:

```text
send C:\path\to\report.pdf
```

The receiving Peer sees the file name, byte size, Downloads destination, and conflict-resolved final path before choosing `accept` or `reject`. Use `destination <path>` at that prompt to override the destination for that Transfer Offer only. Accepted bytes are streamed into destination-local temporary storage, checked with SHA-256 and a byte count, and atomically published without overwriting an existing file. Rejected or mismatched content is removed, and the source is never modified.

Transfer Offers are serialized in session order, so later `send` commands wait in memory while one offer is being reviewed or transferred. Every offer requires a separate receiving-Peer decision, and queued offers are discarded when the Transfer Session ends. Additional connection attempts receive a busy outcome while a session is pending or active.

An inactive Transfer Session warns after 14 minutes and disconnects after 15 minutes. Type `keep-alive` to deliberately keep it open. Active transfers do not expire for idleness.

Use `disconnect` to end the temporary Transfer Session and `quit` to exit. Reconnecting always creates fresh in-memory credentials, discards in-memory queues, and requires verification again.

Use `--listen IPv4:port` to select a local IPv4 bind address or port for the current run. Port `0` selects an available dynamic port, which is the default.

## Test

```powershell
go test ./...
go test -race ./...
```

The integration suite builds and launches real Transferly processes over loopback and interacts with their public terminal interface. On a multicast-capable local network, opt into the two-process real mDNS check:

```powershell
$env:TRANSFERLY_TEST_MDNS = "1"
go test ./integration -run TestRealMDNSDiscoveryConnectsByAvailablePeerNumber
```
