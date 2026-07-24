# Transferly

Transferly enables direct file exchange between reachable computers without cloud storage or a Transferly-operated relay.

## Language

**Direct Transfer**:
A copy of filesystem content over a directly reachable network path between two Peers, without cloud storage or a Transferly-operated relay. The path may be a LAN or a user-managed VPN, and the source remains unchanged.
_Avoid_: Local Transfer, cloud transfer, move

**Peer**:
A computer running Transferly that can participate in a Direct Transfer. Sending and receiving are temporary roles rather than distinct kinds of Peer.
_Avoid_: Client, server

**Available Peer**:
A Peer currently running Transferly in a foreground terminal and advertising its availability on the local network. Availability does not imply identity or trust.
_Avoid_: Online user, trusted device

**Transfer Session**:
A temporary, verified connection between two Peers that may carry multiple Transfer Offers. Verification and trust expire when the connection ends and are never remembered for later sessions.
_Avoid_: Trusted relationship, paired device

**Transfer Offer**:
A proposal from one Peer to another containing any mix of files and recursively transferred folders. The entire batch requires one explicit acceptance before any file content is written; individual files complete independently, so an offer may partially complete.
_Avoid_: Upload request, transfer notification
