# Secure sessions with ephemeral TLS identities

Transferly uses direct TCP connections protected by TLS 1.3, with temporary credentials generated in memory for each Transfer Session. Both users compare and confirm a six-digit code derived from the encrypted handshake before any offer or file metadata is exchanged; no identity or trust state survives the session. This favors an accountless, offline, stateless trust model over persistent pairing, trust-on-first-use, or a custom cryptographic transport.
