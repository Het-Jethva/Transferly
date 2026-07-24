# Transferly v1 release validation

A release is publishable only when every automated gate is green, the Windows artifact has a valid Authenticode signature, and the physical two-laptop matrix below has recorded evidence. Keep evidence in the GitHub release/Actions run and the release checklist; Transferly itself must not create persistent logs.

## Automated pass/fail gates

Run from a clean checkout with Go 1.22 or newer:

```powershell
go test ./...
go test -race ./...
go test ./internal/session -run=^$ -fuzz=^FuzzProtocolFrameLengthHandling$ -fuzztime=30s
go test ./internal/session -run=^$ -fuzz=^FuzzVerificationCodeDerivation$ -fuzztime=30s
go test ./internal/terminal -run=^$ -fuzz=^FuzzManifestPathConfinement$ -fuzztime=30s
go test ./internal/terminal -run=^$ -fuzz=^FuzzConflictNameGeneration$ -fuzztime=30s
go test ./internal/terminal -run=^$ -fuzz=^FuzzTransferOfferManifestLimits$ -fuzztime=30s
go test ./internal/session -run=^$ -bench=^BenchmarkThroughput$ -benchtime=3x -benchmem
```

Pass means every command exits zero. Preserve the command output as evidence. The process suite builds and drives real executables over loopback; it covers verification rejection, incompatible versions, hostile paths and metadata, approval, bidirectional and queued offers, cancellation, connection/process loss, staging cleanup, bounded large-file streaming, and source/destination integrity. Fuzzing is deliberately limited to framing, verification-code derivation, path confinement, conflict naming, and manifest bounds.

On Windows, check reproducibility and portability:

```powershell
./scripts/build-release.ps1 -Version v1.0.0 -OutputDirectory dist
./scripts/verify-portable.ps1 -Executable dist/transferly-windows-amd64.exe -ExpectedVersion v1.0.0
```

Pass means two consecutive release builds had the same SHA-256, the payload is an AMD64 PE executable, `--version` runs in an isolated directory without installation or a separate runtime, and it creates no extra file. Record the printed unsigned-payload hash. Authenticode timestamping changes the final bytes, so reproducibility is checked before signing.

## Signing and publication

The protected GitHub `release` environment must contain:

- `WINDOWS_SIGNING_CERTIFICATE_BASE64`: base64-encoded code-signing PFX
- `WINDOWS_SIGNING_CERTIFICATE_PASSWORD`: PFX password

Require environment approval and restrict tag creation to maintainers. Do not place the PFX, password, or decoded certificate in the repository or an artifact.

Push a `vX.Y.Z` tag, or run **release** manually with that version. `.github/workflows/release.yml` tests and rebuilds the payload, requires both secrets, signs with SHA-256 and an RFC 3161 timestamp, runs `signtool verify /pa /all` and `Get-AuthenticodeSignature`, rechecks portability, and only then uploads/publishes the executable and signed SHA-256 file. A missing or invalid signature fails before artifact upload. Record the Actions URL, signer subject, signature status, final hash, and release URL.

For a local ceremony on a protected Windows host:

```powershell
./scripts/sign-release.ps1 -Executable dist/transferly-windows-amd64.exe -PfxPath X:\secure\transferly.pfx -PfxPassword $env:TRANSFERLY_PFX_PASSWORD
Get-AuthenticodeSignature dist/transferly-windows-amd64.exe | Format-List Status,StatusMessage,SignerCertificate,TimeStamperCertificate
```

Pass requires `Status: Valid`. Never publish an unsigned fallback.

## Two-laptop matrix

Use two physical Windows x64 laptops, covering Windows 10 and Windows 11 across the pair. Use only the signed candidate. Record OS build, Transferly/wire versions, endpoint, network adapter/link speed, source/destination drive model, free space, Defender result, and start/end time. Capture terminal screenshots or copied terminal output outside Transferly.

Run every row:

| Path | Setup and expected result |
| --- | --- |
| mDNS on LAN | Both Peers appear by computer name and IPv4 as untrusted hints; connect by number. |
| Manual endpoint | Disable/block multicast discovery; `connect IPv4:port` still verifies and transfers. |
| Windows Firewall | Deny inbound once and confirm actionable terminal failure; allow the signed app on the intended profile and retry without Transferly elevating or changing policy. |
| Ethernet | Use a router/switch or direct cable and complete the checklist below. |
| Mobile Hotspot offline | Disconnect internet, enable Windows Mobile Hotspot, join the other laptop, and complete a Direct Transfer. |
| User-managed VPN | Use a reachable VPN IPv4 endpoint (manual connection is acceptable) and complete a Direct Transfer. |

For each usable path, compare the six-digit code on both screens, reject one deliberate mismatch, reconnect with a fresh code, then confirm. Verify no offer metadata appears before both confirmations and an incompatible build fails before verification without downgrade.

In one verified Transfer Session:

1. Send a mixed folder/file batch, including nested and empty folders, a hidden file, Unicode and zero-byte names, and an executable/script warning. Review `details`, destination, counts, omissions, conflict-resolved paths, and reject it; confirm no destination writes.
2. Offer again, override the destination, accept, and compare sizes/SHA-256, timestamps, hidden/read-only attributes, and source bytes. Confirm existing names are neither overwritten nor merged.
3. Send in the opposite direction, then queue multiple offers and confirm serialized order and explicit approval for each.
4. Cancel an active large offer with `Ctrl+C` and with `cancel` from the other Peer. Confirm the Transfer Session remains usable, completed files remain, and incomplete/staging data is removed.
5. Disconnect power/network during another large offer. Confirm no incomplete file is published, stale staging is offered for cleanup after restart, and a fresh verified session is required rather than resume.
6. Exercise a changed/vanished source and a destination failure. Confirm exact partial-completion outcomes and retention of independently completed files.
7. Attempt a second connection while occupied, wait through the 14-minute warning, use `keep-alive`, and separately verify 15-minute idle expiry. Confirm active transfer time is exempt.
8. Transfer one file larger than available RAM and a batch of at least 10,000 small files. Record peak working set, CPU, disk, network, duration, failures, and final hashes/counts.

After exit, verify the working directory and user profile contain no Transferly configuration, identity, trust, history, log, telemetry, update, service, or startup state. Destination content and `.transferly-staging` used for crash recovery are the only expected operational filesystem effects.

## Throughput gate

On the same path and drives, avoid Wi-Fi rate changes and other traffic. Run at least three trials each for:

1. raw TCP between the laptops (for example `iperf3`),
2. sequential source read speed,
3. sequential destination write speed, and
4. Transferly with one large incompressible file (at least 4 GiB, larger than RAM where practical).

Convert all medians to bytes/second. Let the bottleneck be the minimum of raw TCP, source read, and destination write medians. Pass only when:

```text
median Transferly bytes/second / bottleneck bytes/second >= 0.85
```

Record tools/versions, commands, all trial values, the calculation, CPU, peak memory, adapter/link rate, and file SHA-256 before/after. Also retain `BenchmarkThroughput` output for the one-large-file and many-small-files cases; it is regression evidence, not a substitute for the physical 85% gate.

## Release decision record

Record each automated command as pass/fail, the matrix rows and checklist evidence, throughput ratio, any deviations, Actions run, signed artifact hash, signer, approver, and final **GO/NO-GO**. Any failed security, signature, cleanup, source-immutability, destination-confinement, or throughput gate is **NO-GO**.
