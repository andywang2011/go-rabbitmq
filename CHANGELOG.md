# Changelog

## Unreleased

### Added

- English `doc.go`, `errors.go`, expanded README and examples.
- `URLs` failover, `RetryConfig`, `NewConnWithRetry`, `NewProducerWithRetry`, `NewConsumerWithRetry`.
- `PrefetchCount` / `PrefetchGlobal` (Qos), `Heartbeat`, `DialTimeout`, `AMQPConfig` override.
- `ErrorHandler` for consumer handler errors; `ConsumerHandle` / `WrapConsumer` with `NotifyClose`.
- Injectable `amqpDial` / `connectConn` hooks; CI coverage gate ≥ 85%.

### Changed

- `Validate` applies `PrefetchCount` default `1`.
- `Publish` / consume paths return `ErrNotInitialized` instead of fmt errors where appropriate.

### Compatibility

- `NewConn`, `NewProducer`, `NewConsumer` signatures unchanged.
- `Producer` and `Consumer` interface method sets unchanged.
