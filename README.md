# xmip-core-transport-mllp

HL7 Minimal Lower Layer Protocol over TCP: a framed message is one Stream, and the acknowledgment travels back on the same connection. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Receive Location keeps its listener, bound on the first receive: a peer that connects between two receives is queued and taken by the next, where until 2026-09-27 each receive bound a listener of its own and a peer between receives was refused. Since 2026-10-21 it keeps its senders' connections too (`transport::serving::Serving`), each frame one receive's turn from whichever sender sends first, and a Send Location keeps one connection per target (`transport::Pool`), frame after frame, each acknowledgement read before the next frame goes; until then every send opened a connection of its own, and every receive accepted one. A connection waiting for its acknowledgement takes no next frame (`src/peer.rs`): what its sender says meanwhile waits in the socket.

## Acknowledgement

The caller holds the connection open for the HL7 acknowledgement, so it is
told after the whole receive cycle. Each framed message arrives whole. On
Accepted the receiver writes an `AA` (application accept) back on the
connection; on Refused an `AE` (application error: understood and refused),
which tells the sender not to send it again; on Failed an `AR` (application
reject: not processed for a reason other than its content), which tells the
sender to send it again. The codes are HL7 v2 Table 0008 (MSA-1), under the
original acknowledgment rules of chapter 2, section 2.9.2. All three are
composed by `hl7_v2::acknowledge`, from the message they answer. A connection
dropped without a verdict is closed unanswered, and the sender sends again.
A send writes the block's start byte, then the Message as it is read from the
Ledger, a piece at a time, never whole in memory, then the block's end
(`Sending::Streamed`, 2026-10-19). It reads the answer's MSA-1: `AR` or `CR`
fails it as retryable, `AE` or `CE` as permanent.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
