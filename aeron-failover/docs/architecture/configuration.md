# Configuration reference

## Sources

- [`../config.properties`](../../config.properties) is the local runtime configuration.
- [`../config.properties.example`](../../config.properties.example) is the annotated native-deployment template.
- [`../../docker/config.properties`](../../docker/config.properties) is the Docker Compose configuration, with container DNS names and persistent-volume paths.

Java programs read the file selected by `AERON_DEMO_CONFIG`; `run.sh` defaults to the project `config.properties`. The C publisher reads the same environment variable and otherwise looks for `../aeron-failover/config.properties` relative to the `shm-udp` directory.

## Setting groups

| Prefix | Purpose |
|---|---|
| `cluster.*` | Ordered member hostnames, per-member data root, UDP port block layout, health endpoint range/timeouts, and Aeron term length. |
| `aeron.*` | Aeron local directory prefix; empty selects the platform default. |
| `proxy.*` | Stable TCP observer endpoint, UDP producer ingress, leader-status endpoint/bind address, outbox path, queues, batch size, retry timing, reconnect delay, poll timing, metrics interval, and verbosity. |
| `monitor.*` | Current-leader status host and ping cadence/timeouts/miss threshold. |
| `subscriber.*` | Saved cursor, connection/reconnect timeout, and verbosity. |
| `publisher.*` | Target sends per second (`0` means unpaced), persistent ID reservation path/block size, and rate-report interval. |

## Host-specific guidance

- Every cluster member must use the same ordered `cluster.hosts` list and matching port layout. Member ID is the command-line argument (`0`, `1`, or `2`).
- Keep each member's `cluster.data-dir/member-N` on that member's own persistent storage. Never share or copy one member's live cluster directory into another.
- Keep `proxy.outbox-file` and the adjacent `.commit` file together on persistent proxy storage. Run only one proxy process against a given outbox.
- `proxy.host` is what the C publisher and TCP subscriber use. It must resolve from their network namespace.
- The default leader-status bind is loopback-only. If the monitor is in another container, the Compose config binds it to the Compose network; do not publish status port 5002 to an untrusted external network.
- Relative paths are resolved relative to the process working directory. Docker config uses absolute paths under `/data` to avoid ambiguity.

The payload length and three-voter topology are protocol assumptions in the demo; changing them requires coordinated code and protocol updates, not just a property edit.
