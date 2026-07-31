# Archive Main Worker (RMQ to ClickHouse)

A high-performance, concurrent, batch-processing Go consumer service that
ingests processed emails from RabbitMQ, uploads their contents to a
distributed storage server, resolves domain metadata, and bulk-saves
records into a ClickHouse data lake.

* **Source & documentation:** https://github.com/Yukthi-Systems/Archive-Main-Worker-RMQ
* **License:** GNU General Public License v3.0

## Quick Start

Create a `.env` file with your configuration (see the
[`.env.example`](https://github.com/Yukthi-Systems/Archive-Main-Worker-RMQ/blob/main/.env.example)
in the source repo for the full list of variables), then run:

```bash
docker run -d \
  --name archive-worker \
  --env-file .env \
  -v $(pwd)/logs:/app/logs \
  rjyspl/archive-main-worker-rmq:latest
```

Or with Docker Compose:

```yaml
services:
  archive-main-worker-rmq:
    image: rjyspl/archive-main-worker-rmq:latest
    container_name: archive-main-worker-rmq
    restart: always
    env_file:
      - .env
    volumes:
      - /var/log/archive-main-worker:/app/logs
```

```bash
docker compose up -d
```

## Configuration

The container is configured entirely through environment variables (no
config files, no baked-in secrets) — RabbitMQ connection details,
ClickHouse connection details, the domain-metadata API, archive-storage
API key, in-memory cache TTLs, and log rotation settings. See the
[Environment Variables Configuration](https://github.com/Yukthi-Systems/Archive-Main-Worker-RMQ#environment-variables-configuration)
section of the README for the full reference table and defaults.

## Tags

* `latest` — most recent build from `main`.
* `<git-tag>` — images are also tagged to match each pushed git tag/release.

## Health Check

The image ships with a `HEALTHCHECK` that verifies the consumer process is
running:

```bash
docker inspect --format='{{json .State.Health}}' archive-worker
```

## Logs

Logs are written as structured JSON to `/app/logs` inside the container
(mount a volume there to persist and inspect them), with correlation IDs
(`batch_id`, `message_id`, `domain_id`) included for debugging.

## Support

* Issues & contributing: [GitHub repository](https://github.com/Yukthi-Systems/Archive-Main-Worker-RMQ)
* Discord: [discord.com/invite/2BS7Z4FhJ](https://discord.com/invite/2BS7Z4FhJ)
* Email: [connect@yukthi.com](mailto:connect@yukthi.com)
