# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Moved the storage-server API key (`ARCHIVE_API_KEY`) out of source code and into environment-based configuration, matching all other secrets.

## [0.1.0] - Initial release

### Added
- Concurrent, batch-processing RabbitMQ consumer that resolves domain metadata, uploads mail content to a storage server, and bulk-inserts records into ClickHouse.
- In-memory TTL cache for domain metadata lookups.
- Dead-letter routing for messages that fail storage upload or database insertion.
- Structured, rotating file logging via zerolog and lumberjack.
- Docker image and Docker Compose deployment support.
