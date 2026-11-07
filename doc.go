// Package rabbitmq provides a thin, testable wrapper around RabbitMQ producers and consumers.
//
// Design constraints (for use in application frameworks):
//   - No viper, no global singletons, no log.Fatal inside the library.
//   - Producer and Consumer interfaces are mock-friendly.
//   - Start consumers from a dedicated cmd process (not inside HTTP servers); see README.
//   - Call Close on shutdown; use NotifyClose on the connection for reconnect logic in your app.
//
// High availability: multiple broker URLs (failover), startup retry via NewConnWithRetry / NewProducerWithRetry.
// High concurrency: ConsumeWithConcurrency plus PrefetchCount (Qos) tuning.
package rabbitmq
