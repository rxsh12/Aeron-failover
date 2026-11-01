# Aeron Cluster failover demo

This project connects the existing `shm-udp/publisher.c` producer role to a three-member Aeron Cluster pipeline through a UDP ingress and a proxy. The old shared-memory one-integer-per-second publisher behavior has been replaced by 100-byte generated messages. `shm-udp/subscriber.c` remains the legacy shared-memory example; it is not the Aeron subscriber.

## What stays stable

The C producer sends 100-byte datagrams to the proxy's UDP ingress (`5003` by default). It does not wait for a receipt, retry, or perform a per-message persistence operation. The proxy validates datagram size, places received payloads into a bounded queue, then appends batches to its checksummed local outbox with one journal sync per batch. Duplicate IDs received by the proxy are deduplicated. The proxy forwards outbox records to Aeron Cluster and retains/retries them until the clustered service's response is received; it syncs a commit cursor before considering the outbox entry complete. The proxy reports datagrams received, journaled, dropped, and the current queue depth.

The payload layout is exactly 100 bytes: 8-byte big-endian message ID, 8-byte big-endian Unix timestamp in nanoseconds, and 84 deterministic test bytes derived from the ID. The C publisher reserves IDs in durable blocks so a restart skips unused IDs rather than reusing them. Run only one producer against an outbox at a time; it is a single-writer ID stream. The proxy's `missingIds` is a live gap estimate: late datagrams reduce it, and unused IDs from a producer restart's reserved block appear as gaps.

For exact end-to-end measurement, `publisher.sent-capture-file` records each 100-byte payload after the local UDP `send()` succeeds. The proxy outbox stores each received payload after durable journaling. After stopping the producer, compare them with:

```sh
python3 ../shm-udp/compare_payloads.py \
  ../shm-udp/data/publisher-sent.bin \
  ./data/proxy-outbox.bin
```

The comparison validates outbox record checksums and reports byte-for-byte matches and missing payload bytes. It compares unique message IDs, so run one producer and use a capture/outbox pair from the same test. The capture is appended to across runs; use fresh, separate capture and outbox paths for isolated measurements. Capturing adds local disk writes and can affect throughput. A process or machine crash can leave buffered capture writes unflushed; a clean producer shutdown is required for a valid result. This measures delivery to durable proxy storage, not merely UDP socket acceptance and not downstream cluster commit.

Downstream observers connect to the proxy host and the configured TCP port (`5001` in the native example) and receive cluster stream sequence numbers. This feed is best effort only. It is not part of the producer-to-cluster delivery path and does not acknowledge application processing.

The third host C is a normal Aeron Cluster voter. A separate monitor process runs on the proxy/observer host. The proxy exposes its current leader ID on a loopback-only status port (`5002` by default), updating it from Aeron Cluster's current leader state while polling; the monitor reads that ID and pings the current leader every second, including immediately after it starts once a leader is available. It reports leader changes and PONG/missed-pong status in the monitor terminal. After three missed pongs, it reports the current leader as unreachable. This health report does not trigger an election, promote a member, or advance/skip stream sequences. Aeron Cluster independently uses its own consensus and failure-detection rules. Three voters allow two surviving members to maintain quorum if one member fails. If two members stop, the remaining member cannot form a majority; the cluster stops accepting work until another voter returns.

The demo's TCP proxy is a separate component and must run on a host that remains available when either service member fails. Its advertised address is what clients use. The proxy and subscriber use the `proxy.tcp-port` setting from the config file. For production, make this endpoint highly available as well (for example with a TCP load balancer or a floating IP); Aeron Cluster does not move an IP address between hosts.

## Latency profile

The C publisher defaults to a 10,000 messages/s pacing target, configured by `publisher.target-messages-per-second`; `0` disables pacing. This is a target, not a throughput guarantee. The publisher reports successful local UDP sends and local send failures. That counter does not prove proxy receipt. The proxy reports UDP receive/journal rates, queue drops, IDs currently missing from its accepted ingress stream, late gap recovery, duplicate IDs, and current receive-queue depth; its cluster terminal reports cluster commit responses/s. The current cluster forwarding path has one in-flight request, so cluster throughput can trail UDP ingress and the outbox can grow. For a benchmark, warm up first and record all rates and outbox queue depth over the same steady-state interval, then repeat while stopping one voter and restarting it. Compare outbox growth (`wc -c ./data/proxy-outbox.bin`; records are 108 bytes) with the commit cursor (`cat ./data/proxy-outbox.bin.commit`) to estimate queued backlog; committed records remain in the journal. Record hardware, filesystem, JVM, network, CPU, and failover conditions. Do not treat any sample as a guarantee or benchmark result.

The proxy retries an unacknowledged cluster request after the configured `proxy.request-retry-timeout-ms` and resends it after a leader change. The proxy outbox deduplicates all previously journaled IDs; the clustered service suppresses a retry of the currently in-flight ID so an ambiguous response does not double-increment the stream. Its Aeron polling loop uses busy-spin (CPU intensive); downstream TCP writes use separate threads and bounded queues.

Failover detection is separate from steady-state message latency. Shortening Aeron Cluster heartbeat/election timeouts can reduce failover time, but increases false elections when the network or host is briefly delayed. The current program leaves those settings at Aeron's defaults; tune them only after measuring the real network and failure modes.

## Multicast and delivery semantics

Aeron Cluster still uses Aeron UDP transport for cluster ingress, replication, and consensus; the producer-to-proxy link is UDP, and observer-facing links use TCP. Multicast can be used as an Aeron transport optimization where supported, but does not elect a leader or provide failover ownership.

Fire-and-forget UDP is best effort. A datagram may be lost in the sender's network stack, in the network, or at the receiver before it is journaled; there is no ACK or retransmission, so the sender cannot know or prevent that loss. Message IDs let the consumer/proxy detect gaps, and duplicate IDs are deduplicated once they reach the proxy. After a datagram is journaled, the proxy retains/retries it until the cluster acknowledges it. This downstream durability is conditional on proxy host/storage availability, persistent member data, and a three-voter quorum. A three-voter cluster needs two voters to retain quorum. The design does not provide an end-to-end zero-loss guarantee with fire-and-forget UDP.

The append-only outbox and commit cursor must remain on persistent local storage and must not be deleted or replaced on proxy restart. The journal currently retains committed records to support duplicate-ID checks; it grows without bound and needs an operational retention/compaction policy before long-running production use. TCP observer clients join the live stream, can miss data while disconnected or when their bounded queue fills, and do not participate in the producer durability guarantee. The subscriber's saved cursor and gap reports are observational, not processing acknowledgements or replay.

## Requirements

- JDK 21+
- A C compiler for the UDP publisher
- Three machines on a mutually reachable private network (or three isolated local processes for development)
- Internet access on first build so the Maven wrapper can download Maven and dependencies
- Open the ports configured in `config.properties`. By default, each member uses UDP ports `9000 + memberId * 100 + 1..5`; allow traffic between all members. The proxy needs outbound access to client-facing member ports (`9002`, `9102`, `9202`), inbound UDP `5003` from the C producer, TCP `5001` from optional observers, and loopback TCP `5002` for the monitor.

## Configuration

All runtime settings are in [`config.properties`](config.properties). The commented [`config.properties.example`](config.properties.example) explains each key; copy it to `config.properties` when setting up a fresh checkout. Edit the same settings on each machine, especially `cluster.hosts`, `proxy.host`, and the paths. Keep member storage local and persistent; do not share a member directory between machines. Relative paths are resolved from each process's working directory: Java commands run from `aeron-failover/`; run the C publisher from `shm-udp/`.

The properties cover cluster host addresses, data/Aeron directories and cluster port layout; proxy endpoints, outbox path, queue sizes, batching, retries and verbosity; monitor ping cadence and timeouts; subscriber cursor/reconnect behavior; and producer pacing, ID allocation, and optional sent-payload capture. The payload is fixed at 100 bytes and the cluster at three voters; these are protocol invariants rather than tunable settings. To use a non-default configuration file, set `AERON_DEMO_CONFIG=/absolute/path/config.properties` for both `run.sh` and the C publisher.

## Architecture and operations documentation

The detailed documentation is organized under [`docs/architecture/`](docs/architecture/README.md): topology, delivery behavior, failover timing, configuration, operational runbook, and Docker deployment.

## Docker

The Docker Compose setup builds the Java services and C UDP publisher into one image, then runs the three independent cluster voters, stable proxy, and monitor. Publisher and subscriber are opt-in profiles. See [`docs/architecture/docker.md`](docs/architecture/docker.md) for network/persistence caveats.

Edit `docker/config.properties`, then from this directory run:

```sh
docker compose -f docker-compose.yml up --build -d
docker compose -f docker-compose.yml --profile producer up -d publisher
docker compose -f docker-compose.yml logs -f proxy publisher
```

By default the proxy publishes TCP `5000` and UDP `5003` on all host interfaces. Restrict that exposure with `PROXY_PUBLISH_HOST` or the host firewall. This Compose configuration runs all cluster members on one Docker host; it is for packaging and local testing, not host-level high availability. The native example uses TCP `5001` because macOS may reserve `5000` for Control Center.

## Build

From this directory:

```sh
./mvnw -DskipTests package
```

The shaded jar is `target/aeron-failover-1.0.0.jar`. Use `./run.sh` to provide the JVM export required by Aeron's low-level runtime on modern JDKs.

## Run on three hosts

Choose fixed private addresses reachable by every node. The same ordered host list must be used on all three nodes:

```sh
# Edit cluster.hosts in config.properties:
# cluster.hosts=10.0.0.11,10.0.0.12,10.0.0.13
```

On host 0, run member 0 (Server A):

```sh
./run.sh demo.aeron.ClusterNode 0
```

On host 1, run member 1 (Server B):

```sh
./run.sh demo.aeron.ClusterNode 1
```

On host 2, run member 2 (voter C) and the stable proxy:

```sh
./run.sh demo.aeron.ClusterNode 2
./run.sh demo.aeron.TcpProxy
```

On the proxy/observer host, run the standalone monitor in its own terminal, after starting the proxy. The monitor needs access to the proxy's loopback leader-status port (default `5002`). Its PING/PONG output appears in this terminal, not the proxy terminal:

```sh
./run.sh demo.aeron.ClusterMonitor
```

Each member must use persistent, member-specific `data/member-N` storage. Do not copy one member's cluster directory to another member. Set `cluster.data-dir` in the configuration to select another root; all members should use the same configured root path on their respective machines. Keep the proxy outbox and its adjacent `.commit` cursor on persistent storage; do not start concurrent proxy instances against the same outbox.

On the C publisher host, from `shm-udp/`, compile and run:

```sh
cc -O3 -std=c11 -Wall -Wextra -Wpedantic -o publisher publisher.c
mkdir -p data
./publisher
```

Set `proxy.host`, `proxy.producer-udp-port`, `publisher.target-messages-per-second`, `publisher.id-state-file`, and `publisher.sent-capture-file` in `config.properties`. A target rate of `0` means unpaced sending. The publisher's ID state survives restarts and reserves IDs in blocks. Keep that file. Its periodic rate is successful local UDP sends/s, not proof of proxy receipt or cluster commit. Compare it with the proxy's `received` and `journaled` rates; `dropped` counts malformed packets and queue-overflow drops visible at the proxy, not datagrams lost before the proxy. The optional capture file contains raw sent payloads; use `python3 ../shm-udp/compare_payloads.py ../shm-udp/data/publisher-sent.bin ./data/proxy-outbox.bin` from `aeron-failover/` after a clean producer shutdown to measure payload bytes missing before durable proxy journaling. Use fresh capture and outbox files for a standalone test.

On each downstream machine:

```sh
./run.sh demo.aeron.TcpSubscriber
```

Configure `proxy.host`, `proxy.tcp-port`, and `subscriber.cursor-file`. Clients always connect to the stable proxy endpoint and do not need to discover who is leader. The member terminals label the elected leader with `IS LEADER`. The monitor terminal prints PING/PONG only for the current leader and switches its health check when Aeron elects another member. The proxy terminal separately prints cluster leader changes and retries any in-flight producer message using the same ID. If one member stops, the other two retain quorum and Aeron can elect one of them. If two members stop, the remaining member cannot become leader alone; there is no quorum. The monitor threshold is not a sequence-loss setting. This demo does not include subscriber acknowledgements or replay after a disconnect, so it does not guarantee no lost observer-side application work during failover. Restart a failed member with its original persistent data directory to see it rejoin. Only one cluster leader is active at a time.

The subscriber's `data/last-sequence.txt` is its last observed sequence. It receives a best-effort observational feed only. A gap report does not prove producer loss or business-processing loss; this endpoint provides no replay or downstream processing acknowledgement.

## Local three-member test

For development only, set `cluster.hosts=127.0.0.1,127.0.0.1,127.0.0.1` and launch three member terminals using member IDs 0, 1, and 2. Run the proxy, monitor, optional observer, and publisher in separate terminals. For realistic failure-domain validation, use three hosts: local processes cannot demonstrate host/network isolation.

## Source

The message-passing model follows the [Aeron message-passing overview](https://aeron.io/docs/distributed-systems-basics/message-passing/). Aeron Cluster's service lifecycle and client API are based on the [Aeron Cluster sample code](https://github.com/aeron-io/aeron/tree/master/aeron-samples/src/main/java/io/aeron/samples/cluster).
