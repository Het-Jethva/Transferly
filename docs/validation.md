# Transferly validation

Transferly is developed and distributed from source. The automated suite covers the public terminal workflow over real loopback processes, while the optional checks below exercise network and hardware behavior that hosted CI cannot reproduce.

## Automated checks

Run from a clean checkout with Go 1.26 or newer:

```powershell
go test ./...
go test -race ./...
go test ./internal/session -run=^$ -fuzz=^FuzzProtocolFrameLengthHandling$ -fuzztime=30s
go test ./internal/session -run=^$ -fuzz=^FuzzVerificationCodeDerivation$ -fuzztime=30s
go test ./internal/terminal -run=^$ -fuzz=^FuzzManifestPathConfinement$ -fuzztime=30s
go test ./internal/terminal -run=^$ -fuzz=^FuzzConflictNameGeneration$ -fuzztime=30s
go test ./internal/terminal -run=^$ -fuzz=^FuzzTransferOfferManifestLimits$ -fuzztime=30s
go test ./internal/session -run=^$ -bench=^BenchmarkThroughput$ -benchtime=3x -benchmem
golangci-lint run ./...
```

Every command must exit zero. The process suite covers verification rejection, incompatible versions, hostile paths and metadata, approval, bidirectional and queued Transfer Offers, cancellation, connection loss, staging cleanup, bounded large-file streaming, and source/destination integrity.

Collect coverage from the built Peer processes with:

```powershell
$env:TRANSFERLY_COVERDIR = "$PWD/covdata"
go test ./... -coverpkg=./internal/...,./cmd/...
go tool covdata percent -i=covdata
```

## Portable Windows build

Build twice, compare the resulting hashes, exclude fault-injection code, and verify the portable executable in an isolated directory:

```powershell
./scripts/build-windows.ps1 -Version v0.0.0-local -OutputDirectory dist
./scripts/verify-portable.ps1 -Executable dist/transferly.exe -ExpectedVersion v0.0.0-local
```

## Optional two-computer checks

Use two Windows 10/11 x64 computers on a network you control. These are manual confidence checks, not automated publication gates.

- Discover both Peers over mDNS on a LAN, then repeat using a manual IPv4 endpoint with multicast unavailable.
- Compare and reject one verification-code mismatch, reconnect, compare matching codes, and confirm on both Peers.
- Transfer mixed files and folders in both directions; verify sizes, SHA-256 digests, timestamps, hidden/read-only attributes, and source immutability.
- Reject one Transfer Offer and confirm that no destination content is written.
- Cancel an active large Transfer Offer from each side and confirm that completed files remain while incomplete staging is removed.
- Interrupt a transfer by ending the process or network path; confirm stale staging is detected and a fresh verified Transfer Session is required.
- Exercise an existing destination name, a changed source, an executable/script warning, an empty file, and a batch with many small files.
- Confirm Transferly creates no configuration, identity, trust, history, log, telemetry, service, or startup state.

For a real mDNS check from the Go suite:

```powershell
$env:TRANSFERLY_TEST_MDNS = "1"
go test ./integration -run TestRealMDNSDiscoveryConnectsByAvailablePeerNumber
```

## Optional throughput check

On the same network path and drives, compare median Transferly throughput for a large incompressible file against the slowest measured limit among raw TCP, source read, and destination write throughput. The project target is at least 85% of that bottleneck. Record the tools, commands, trial values, CPU, memory, link rate, and source/destination hashes when using this check as performance evidence.
