# Operations runbook

## Native setup

1. Edit `cluster.hosts` in `config.properties` to the same three private addresses on all hosts. Set proxy/publisher/monitor addresses and persistent paths as appropriate.
2. Build from `aeron-failover/`: `./mvnw package`.
3. Start member IDs 0, 1, and 2 in separate terminals: `./run.sh demo.aeron.ClusterNode 0` (then `1`, then `2`).
4. Start `./run.sh demo.aeron.TcpProxy`.
5. Start `./run.sh demo.aeron.ClusterMonitor` on the proxy host.
6. On the producer host, from `shm-udp/`, compile with `cc -O3 -std=c11 -Wall -Wextra -Wpedantic -o publisher publisher.c`, then run `./publisher`.
7. Optionally start an observer with `./run.sh demo.aeron.TcpSubscriber` from the project directory.

## Docker Compose setup

See [Docker deployment](docker.md). Configure `docker/config.properties`, then run `docker compose -f docker-compose.yml up --build -d`. Start workload profiles explicitly with `--profile producer` or `--profile observer`; the producer continuously sends traffic until stopped.

## Useful measurements

- Compare local publisher sends/s with proxy `received/s` and `journaled/s`.
- Watch proxy `dropped/s`, `missingIds`, and `queued`; the gap figure may shrink when late packets arrive.
- Watch cluster commit responses/s and outbox growth. If journaled ingress persistently exceeds cluster commits, backlog increases.
- To estimate journaled record count, divide outbox byte length by 108 (12 bytes framing/checksum plus the 100-byte payload). Compare this with the persisted commit cursor count. Do not delete the journal or cursor while the proxy is running.
- Test failover by stopping exactly one voter and measuring the time to the proxy's next-leader log. Do not stop a second voter if you expect the cluster to keep committing.

## Common issues

| Symptom | Check |
|---|---|
| Proxy shows no leader / reconnects | Verify all three members started, hosts and port offsets match, and UDP routes/firewalls permit member traffic. |
| Monitor cannot read status | Ensure proxy is running. Native runs use loopback status; Compose monitor uses `proxy` on the private Compose network. |
| Producer local sends exceed proxy receives | Check destination address, UDP firewall, receive queue pressure, and network load. A successful local send is not a receipt. |
| `dropped/s` rises or producer queue grows | Persistence cannot keep up or the queue is too small for the burst. Tune after measuring; queue size does not increase sustainable throughput. |
| Two voters are unavailable | The remaining voter cannot form quorum; restore a voter with its original persistent data. |
| Subscriber reports gaps | The subscriber is a live best-effort observer with no replay; gaps are not a processing acknowledgment or definitive source-loss count. |

Use persistent volumes for data. Do not remove Compose volumes to troubleshoot unless you intentionally want to erase the cluster state and outbox.
