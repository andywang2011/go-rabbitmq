# go-rabbitmq

Thin, testable RabbitMQ producer/consumer wrapper for Go (`github.com/rabbitmq/amqp091-go`).

```bash
go get github.com/andywang2011/go-rabbitmq@latest
```

## Design

- No viper, no global singletons, no `log.Fatal` inside the library.
- `Producer` and `Consumer` interfaces for mocking.
- Optional `ConsumerHandle` / `WrapConsumer` for `Close` and `NotifyClose` helpers.
- `NewProducerWithRetry` / `NewConsumerWithRetry` retry **startup dial + channel open** only (not publish/handler).
- Multiple broker URLs for failover; tune Qos prefetch for high throughput.

## Quick start

```go
import librmq "github.com/andywang2011/go-rabbitmq"

cfg := librmq.Config{
    Host: "127.0.0.1", Port: 5672,
    User: "guest", Password: "guest",
}

p, err := librmq.NewProducer(cfg)
if err != nil { /* handle */ }
defer p.Close()
_ = p.Publish(ctx, "", "my-queue", []byte("hello"), librmq.WithJSON())

c, err := librmq.NewConsumer(cfg)
if err != nil { /* handle */ }
defer librmq.WrapConsumer(c).Close()
_ = c.EnsureQueue("my-queue")
_ = c.ConsumeWithConcurrency(ctx, "my-queue", 4, handler)
```

## Configuration

| Field | Default / notes |
|-------|-----------------|
| `URL` / `URLs` | `URLs` tried in order (failover); else `URL` or built from Host/Port |
| `Timeout` | `10s` |
| `PrefetchCount` | `1` (Qos before consume) |
| `Heartbeat` / `DialTimeout` | Applied to `amqp.Config` when `AMQPConfig` nil |
| `Retry` | `DefaultRetryConfig()` for `*WithRetry` helpers |
| `AMQPConfig` | Full driver override (TLS, etc.) |
| `ErrorHandler` | Called on handler error (default: log) |

## Production (high availability + concurrency)

- **One producer per process**; reuse for all publishes.
- Use `URLs` with multiple broker nodes; pair with `NewProducerWithRetry` on startup.
- Set `PrefetchCount` ≥ worker concurrency for pipelined consumption; use `ConsumeWithConcurrency`.
- Run consumers in a **dedicated process**; on SIGTERM `Close()` the consumer and rebuild on `NotifyClose` in your app (library does not auto-reconnect).
- HTTP services should Wire only `Producer`; start consumers from cmd (see demo-api docs).

## Limitations

- No publisher confirms (Phase 2).
- No automatic reconnect loop inside the library.
- Business publish/handler failures are not retried by the library.

## 中文摘要

RabbitMQ 轻量封装：配置驱动、多 URL failover、启动重试、Qos 预取、可注入单测。`Producer`/`Consumer` 接口不变，新增 `WrapConsumer` 与 `*WithRetry`。无 viper/全局变量，适合 Wire 与高并发消费场景。
