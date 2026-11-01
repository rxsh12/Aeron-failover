# Docker deployment

## Services

The Compose file starts three independent cluster voters (`server-a`, `server-b`, `server-c`), the stable proxy, and the monitor. The publisher and subscriber are opt-in profiles. Each voter and the proxy have separate named persistent volumes; the publisher and observer also have their own cursors.

The Docker build context is the workspace parent because the C publisher source currently lives in the sibling `shm-udp/` folder. `.dockerignore` only includes that publisher source from the sibling tree. The workspace `.gitignore` ignores the entire `shm-udp/` folder.

## Run

From `aeron-failover/`:

```sh
docker compose -f docker-compose.yml build
docker compose -f docker-compose.yml up -d
docker compose -f docker-compose.yml logs -f server-a server-b server-c proxy monitor
```

Start the fire-and-forget generator:

```sh
docker compose -f docker-compose.yml --profile producer up -d publisher
docker compose -f docker-compose.yml logs -f publisher proxy
```

Start the optional observer:

```sh
docker compose -f docker-compose.yml --profile observer up -d subscriber
docker compose -f docker-compose.yml logs -f subscriber
```

Stop only the producer, retaining state:

```sh
docker compose -f docker-compose.yml stop publisher
```

Stop the stack without deleting persistent volumes:

```sh
docker compose -f docker-compose.yml down
```

Do not use `down -v` unless intentionally deleting all cluster, proxy, publisher, and subscriber data.

## Networking

Compose DNS names `server-a`, `server-b`, and `server-c` are used for Aeron membership; `proxy` is the producer and observer destination. Cluster UDP traffic stays on the private Compose network between containers. Only proxy TCP 5000 and UDP 5003 are published to the host by default. Override `PROXY_PUBLISH_HOST` to bind those published ports to a particular host interface.

The proxy leader-status socket binds to `0.0.0.0` only inside the private Compose network so the separate monitor container can reach it. Port 5002 is not published to the host. Restrict access to the Docker network and do not treat this unauthenticated demo endpoint as a production control plane.

Containers provide process isolation and persistent volumes, not multi-host fault domains. This default Compose stack runs on one Docker host; loss of that host takes down every voter and the proxy. A production multi-machine deployment needs separate hosts and network/firewall configuration.

## Persistence

Named volumes keep each member's `data/member-N` path independent and preserve its cluster archive state. The proxy outbox and commit cursor share the proxy volume. Producer ID state and observer cursor are independent volumes. Back up according to the application's consistency requirements; do not copy live member data between member volumes.

## Configuration and rebuild

Edit [`../../docker/config.properties`](../../docker/config.properties) to change Compose DNS names, ports, rates, thresholds, or paths. The file is mounted read-only into each service, so changing it does not require an image rebuild; recreate services for a changed configuration:

```sh
docker compose -f docker-compose.yml up -d --force-recreate
```

Source changes require rebuilding:

```sh
docker compose -f docker-compose.yml build --no-cache
```

The C publisher is compiled into the image from `shm-udp/publisher.c`.
