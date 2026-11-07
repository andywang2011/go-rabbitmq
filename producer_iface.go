package rabbitmq

import "context"

// Producer abstracts message publishing for tests and application wiring.
type Producer interface {
	Publish(ctx context.Context, exchange, routingKey string, body []byte, opts ...PublishOption) error
	Close() error
}
