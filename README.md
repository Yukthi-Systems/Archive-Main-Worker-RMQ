# Archive Main Worker (RMQ to ClickHouse)

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Go Reference](https://img.shields.io/badge/go-1.25%2B-00ADD8?logo=go)](go.mod)
[![Discord](https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white)](https://discord.com/invite/2BS7Z4FhJ)

A high-performance, concurrent, batch-processing Go consumer service designed to ingest processed emails from RabbitMQ, upload their contents to a distributed storage server, resolve metadata details, and bulk-save records into a ClickHouse data lake.

This service is architected to mirror high-concurrency Rust pipeline patterns:
* **Concurrent Ingest Engine**: Messages in a batch are processed concurrently using Go's worker-goroutines.
* **Storage and API Caching**: Fetches domain parameters with an in-memory TTL cache to reduce remote API round-trips.
* **ClickHouse Native Streaming**: Flushes successful processing records using ClickHouse native streaming batches (`PrepareBatch`).
* **Always-ACK Batch Strategy**: Acknowledges processing to RabbitMQ only when the batch collection concludes.
* **Granular Dead-Lettering**: Failed messages are individually pushed to the dead-letter queue with diagnostic headers (`x-storage-success` / `x-clickdb-success`) to allow automated retries at specific steps.

## Table of Contents

* [Environment Variables Configuration](#environment-variables-configuration)
* [Local Setup & Development](#local-setup--development)
* [Docker Integration](#docker-integration)
* [Testing](#testing)
* [Contributing](#contributing)
* [Security](#security)
* [Community](#community)
* [License](#license)

---

## Environment Variables Configuration

The application loads configuration parameters from standard OS environment
variables, falling back to values declared in a local `.env` file if one
exists (environment variables always take precedence). Copy
[`.env.example`](.env.example) to `.env` and fill in your own values —
none of the defaults below are real credentials, they are placeholders.

| Variable Name | Description | Default Value |
|---|---|---|
| **RabbitMQ settings** | | |
| `RB_HOST` | Host address of the RabbitMQ server | `<rabbitmq-host>` |
| `RB_PORT` | Port of the RabbitMQ server | `5672` |
| `RB_USERNAME` | Username for RabbitMQ authentication | `<username>` |
| `RB_PASSWD` | Password for RabbitMQ authentication | `<password>` |
| `RB_VHOST` | Virtual host name inside RabbitMQ | `/` |
| `RB_CONSUME_QUEUE`| Queue to consume message batches from | `archive_processed_mails` |
| `RB_FAILED_QUEUE` | Queue to route failed messages to (dead-letter) | `dead_parser` |
| `RB_CONSUMER_TAG` | Identity tag for this consumer worker instance | `main-consumer-1` |
| `RB_PREFETCH` | Prefetch limit (maximum unacknowledged deliveries) | `100` |
| `RB_WORKER_COUNT` | Number of concurrent batch worker threads | `1` |
| `RB_BATCH_SIZE` | Maximum batch capacity before flushing to databases | `100` |
| `RB_BATCH_FLUSH_TIMEOUT`| Maximum time duration to wait before flushing incomplete batches | `1s` |
| `AMQPR_CON_DELAY` | Time duration to wait between RabbitMQ connection retries | `5s` |
| **ClickHouse settings** | | |
| `CLICKHOUSE_ADDR` | Host:Port address for ClickHouse (HTTP interface) | `<clickhouse-host>:8123` |
| `CLICKHOUSE_USER` | Username for ClickHouse authentication | `default` |
| `CLICKHOUSE_PASSWORD`| Password for ClickHouse authentication | `<password>` |
| `CLICKHOUSE_DB` | ClickHouse target database name | `default` |
| **Domain API settings** | | |
| `API_URL` | Base URL of the API to resolve domain properties | `<api-url>` |
| `API_KEY` | Header key token for authentication | `<api-key>` |
| **Archive storage server settings** | | |
| `ARCHIVE_API_KEY` | Header key token used to authenticate mail uploads to the storage server | `<archive-api-key>` |
| **Caching settings** | | |
| `CACHE_TTL` | Duration domain details are cached in memory | `3h` |
| `CACHE_CLEANUP_INTERVAL`| Interval at which expired cache items are evicted | `5m` |
| **Logging settings** | | |
| `LOG_FILE` | Absolute/relative file path to write application logs | `./logs/app.log` |
| `LOG_MAX_SIZE_MB` | Maximum size in Megabytes of a log file before rotation | `50` |
| `LOG_MAX_BACKUPS` | Maximum quantity of old rotated log files to retain | `5` |
| `LOG_MAX_AGE_DAYS`| Maximum age in days to retain old log files | `14` |
| `LOG_COMPRESS` | Compress rotated log files using gzip (`true`/`false`) | `true` |
| `LOG_LEVEL` | Logging verbosity (`debug`, `info`, `warn`, `error`) | `debug` |
| `LOG_CONSOLE` | Mirror structured log output directly to stdout | `false` |

---

## Local Setup & Development

### 1. Configure the Environment
Copy the example environment configuration:
```bash
cp .env.example .env
```
Modify `.env` to match your local setup. The application automatically loads values from this file on startup.

### 2. Run Locally
Execute the main consumer entry point:
```bash
go run ./cmd/consumer/
```

---

## Docker Integration

### 1. Local Image Build
Build the optimized, multi-stage Docker image locally:
```bash
docker build -t archive-main-worker:local .
```

### 2. Run the Container
Run the worker container, passing environment variables from your local `.env` file and mounting a local directory to capture rotated logs:

```bash
docker run -d \
  --name archive-worker \
  --env-file .env \
  -v $(pwd)/logs:/app/logs \
  archive-main-worker:local
```

### 3. Check Logs & Health Status
View log streams using Docker:
```bash
docker logs -f archive-worker
```

Verify container health status:
```bash
docker inspect --format='{{json .State.Health}}' archive-worker
```
The logs inside the container are kept clean, plain-English, and easy to read by operators, while keeping correlation IDs (`batch_id`, `message_id`, `domain_id`) in the JSON parameters for debugging.

### 4. Docker Compose

A [`docker-compose.yml`](docker-compose.yml) is provided for running the
published image directly. It reads its configuration from the same `.env`
file via `env_file`, so no secrets are baked into the compose file itself:

```bash
cp .env.example .env   # if you haven't already
docker compose up -d
```

---

## Testing

Run the Go test suite:
```bash
go test ./...
```

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)
for the workflow, coding conventions, and how to submit a pull request.
This project follows a [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

To report a vulnerability, please see [SECURITY.md](SECURITY.md) rather
than opening a public issue.

## Community

* Discord: [discord.com/invite/2BS7Z4FhJ](https://discord.com/invite/2BS7Z4FhJ)
* Email: [connect@yukthi.com](mailto:connect@yukthi.com)

## License

This project is licensed under the terms of the [GNU General Public License v3.0](LICENSE).
