# Contributing

Thanks for your interest in improving Archive Main Worker (RMQ to ClickHouse).
This document covers how to set up a development environment, the
conventions used in this codebase, and how to submit a change.

## Getting Started

1. Fork the repository and clone your fork.
2. Install [Go 1.25+](https://go.dev/dl/).
3. Copy the environment template and adjust it for your local RabbitMQ/ClickHouse instances:
   ```bash
   cp .env.example .env
   ```
4. Confirm the project builds and its tests pass:
   ```bash
   go build ./...
   go test ./...
   ```

## Development Workflow

1. Create a branch off `main` with a descriptive name, e.g. `fix/rabbitmq-reconnect-backoff`.
2. Make your changes, keeping commits focused and logically scoped.
3. Run `gofmt -l .` and `go vet ./...` before committing; both must be clean.
4. Add or update tests for any behavioral change. See [internal/cache/cache_test.go](internal/cache/cache_test.go) for the style used in this repo (table-driven, `testing` package only — no external assertion libraries).
5. Update the [README](README.md) if you add, rename, or remove a configuration variable or public-facing behavior.
6. Push your branch and open a pull request against `main`.

## Coding Conventions

* Follow standard Go formatting (`gofmt`) and idioms — the existing packages under `internal/` and `pkg/` are the reference style.
* Exported types and functions must have a doc comment starting with the identifier's name, per the [Go documentation conventions](https://go.dev/doc/comment).
* Wrap errors with context using `fmt.Errorf("...: %w", err)` rather than discarding the underlying error.
* Prefer passing dependencies explicitly (as the existing `Consumer`, `APIClient`, and `ClickHouseDB` types do) over introducing package-level globals.
* Keep `internal/` packages internal — new code shared across module boundaries belongs in `pkg/`.

## Commit Messages and Pull Requests

* Use a short, imperative summary line (e.g. "Add retry backoff for ClickHouse batch inserts").
* Explain *why* a change is needed in the body when it isn't obvious from the diff.
* Reference related issues (e.g. `Fixes #42`) where applicable.
* Keep pull requests focused on a single concern; unrelated cleanups should be a separate PR.

## Reporting Bugs and Requesting Features

Use the GitHub issue templates under `.github/ISSUE_TEMPLATE/`. Include
enough detail to reproduce the problem: relevant log lines (with any
sensitive data redacted), configuration involved, and expected vs. actual
behavior.

## Security Issues

Do not open a public issue for security vulnerabilities — see [SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions will be licensed under
the project's [GNU General Public License v3.0](LICENSE).
