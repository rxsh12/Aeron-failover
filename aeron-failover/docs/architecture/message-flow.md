# Message flow and delivery semantics

## Payload

Every datagram is exactly 100 bytes:

| Byte range | Length | Contents |
|---|---:|---|
| 0-7 | 8 | Positive message ID, unsigned-style big-endian integer. |
| 8-15 | 8 | Generation timestamp in Unix nanoseconds, big-endian. |
| 16-99 | 84 | Deterministic test bytes derived from the ID. |

The ID reservation file reserves blocks in advance. A producer restart skips unused IDs from the prior block rather than reusing them. Those intentionally unused IDs can appear as apparent gaps; compare metrics during one uninterrupted run for a more useful gap estimate.

## End-to-end path

1. The C publisher creates a message, then sends one UDP datagram without a receipt, retry, or receiver-side confirmation.
2. The proxy rejects malformed payload lengths. Valid payloads enter a bounded in-memory queue; queue overflow is counted as a proxy drop.
3. A writer drains batches from that queue, deduplicates IDs against the durable journal, writes checksummed records, then synchronizes the journal.
4. The proxy forwards journaled entries in order to the Aeron Cluster client ingress. Currently, it allows one pending message at a time and waits for the matching service response before advancing its durable commit cursor.
5. The clustered service advances its stream sequence for a new request ID and responds to the proxy. The proxy persists completion and emits the sequence to connected TCP observers.

## What is and is not guaranteed

| Stage | Behavior | Limitation |
|---|---|---|
| Publisher to proxy | Fire-and-forget UDP. | Packets may be lost in the sender, network, or receiver stack. There is no end-to-end ACK or retransmission. |
| Proxy receive queue | Bounded in-memory queue. | Full queue causes explicitly counted local drops. Process failure before journal sync can lose queued entries. |
| Proxy journal to cluster | Durable outbox, sequential forwarding, retry on ambiguous response. | Depends on proxy storage and continued availability; cluster requires quorum. |
| Proxy to subscriber | Best-effort live TCP sequence stream. | No replay or subscriber processing ACK. A disconnected/slow subscriber may miss events. |

The design is therefore **not zero-loss from publisher to consumer**. A message is only protected by cluster retry after the proxy has durably journaled it.

## Interpreting ingress counters

- `received/s`: well-formed 100-byte datagrams accepted by the proxy receiver, before queue admission.
- `journaled/s`: new unique records successfully appended and synced; duplicate IDs do not add records.
- `dropped/s`: malformed datagrams plus datagrams rejected because the in-memory queue was full.
- `missingIds`: count of currently observed ID gaps in accepted queue entries. A late datagram reduces the count; it is not a permanent loss counter.
- `recoveredGaps/s`: rate at which late arrivals filled earlier gaps.
- `duplicateIds/s`: rate of duplicate IDs detected on accepted ingress.
- `queued`: number of datagrams awaiting journal persistence.

The sender's `successful UDP sends` counts only successful local socket calls. It does not prove that the proxy received or journaled each datagram. Outbox journal bytes can measure records persisted by the proxy; its commit cursor measures progress toward cluster commit. Neither alone proves downstream subscriber application processing.
