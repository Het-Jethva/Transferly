# Transferly

Transferly is a foreground terminal application for direct file exchange between reachable Peers. It opens temporary, human-verified Transfer Sessions over a manually supplied IPv4 endpoint and can safely copy one approved regular file at a time.

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

Each Peer prints one or more endpoints:

```text
Endpoint: 192.168.1.20:53144
```

On either Peer, connect to an endpoint printed by the other:

```text
connect 192.168.1.20:53144
```

Both terminals display a six-digit code. Compare the codes through an in-person or otherwise trusted channel, then type `yes` on both terminals only when they match. Type `no` on either terminal if they do not match.

In a verified Transfer Session, either Peer can offer a readable regular file:

```text
send C:\path\to\report.pdf
```

The receiving Peer sees the file name, byte size, Downloads destination, and conflict-resolved final path before choosing `accept` or `reject`. Use `destination <path>` at that prompt to override the destination for that Transfer Offer only. Accepted bytes are streamed into destination-local temporary storage, checked with SHA-256 and a byte count, and atomically published without overwriting an existing file. Rejected or mismatched content is removed, and the source is never modified.

Use `disconnect` to end the temporary Transfer Session and `quit` to exit. Reconnecting always creates fresh in-memory credentials and requires verification again.

Use `--listen IPv4:port` to select a local IPv4 bind address or port for the current run. Port `0` selects an available dynamic port, which is the default.

## Test

```powershell
go test ./...
go test -race ./...
```

The integration suite builds and launches real Transferly processes over loopback and interacts with their public terminal interface.
