# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-11-07

### Breaking

- See repository diff for API changes in this release (secure defaults, renamed packages where applicable, context-aware constructors).

### Security

- Hardened defaults: TLS, timeouts, body limits, PII redaction where logging applies.

### Fixed

- Bug fixes from the v0.2.0 hardening pass.

### Changed

- CI uses go-version-file; added race, staticcheck, govulncheck, golangci-lint (gosec).



### Changed

- **Breaking:** `NewConn`, `NewConnWithRetry`, `NewProducer`, `NewProducerWithRetry`, `NewConsumer`, and `NewConsumerWithRetry` take a `context.Context` first; retries and backoff stop on cancellation.
- `Config.Validate` now rejects invalid values (negative durations/prefetch/redelivery, bad port, non-amqp URLs) and runs before any connection attempt.
- `Config.Timeout` is the TCP dial and handshake timeout when `DialTimeout` is zero.
- Consumer no longer blocks `Close` when ctx is cancelled while the worker queue is full; the undelivered message is requeued.
- Retry helper uses `time.NewTimer` + `Stop`, `math/rand/v2`, and honours ctx between and during attempts.

### Added

- `Config.MaxRedelivery`: failed messages over the limit are rejected with `requeue=false` so the broker can dead-letter them.

## [0.1.0] - 2026-11-07

### Added

- Producer and consumer interfaces with failover URLs and startup retry.
