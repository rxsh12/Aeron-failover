# Architecture documentation

This folder documents the Aeron failover demo's architecture, delivery contract, recovery behavior, configuration, operations, and container deployment.

## Documents

- [System overview](system-overview.md): components, deployment boundaries, and a topology diagram.
- [Message flow and delivery semantics](message-flow.md): payload format, journaling, deduplication, and what the counters mean.
- [Failure detection and recovery](failover.md): Aeron elections versus monitor health reporting, expected timing, and how to measure failover.
- [Configuration reference](configuration.md): configuration files, keys, paths, and host-specific values.
- [Operations runbook](operations.md): native and Docker commands, test procedure, metrics, and troubleshooting.
- [Docker deployment](docker.md): image build, Compose services, persistence, network and security notes.

Start with the project [README](../../README.md) for a concise build-and-run guide.
