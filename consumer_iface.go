package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer abstracts message consumption for tests and application wiring.
type Consumer interface {
	Consume(ctx context.Context, queue string, handler MessageHandler) error
	ConsumeWithConcurrency(ctx context.Context, queue string, concurrency int, handler MessageHandler) error
	EnsureQueue(name string) error
	Close() error
}

// ConsumerHandle adds lifecycle helpers without changing the Consumer interface.
type ConsumerHandle interface {
	// Close stops consumption and releases resources.
	Close() error
	// NotifyClose returns a channel of connection errors when supported by the underlying consumer.
	NotifyClose() <-chan *amqp.Error
}

type consumerHandle struct {
	c Consumer
}

// WrapConsumer returns a ConsumerHandle for graceful shutdown and reconnect hooks.
func WrapConsumer(c Consumer) ConsumerHandle {
	if c == nil {
		return nil
	}
	return &consumerHandle{c: c}
}

func (h *consumerHandle) Close() error {
	if h == nil || h.c == nil {
		return nil
	}
	return h.c.Close()
}

func (h *consumerHandle) NotifyClose() <-chan *amqp.Error {
	if h == nil || h.c == nil {
		return nil
	}
	if n, ok := h.c.(interface{ NotifyClose() <-chan *amqp.Error }); ok {
		return n.NotifyClose()
	}
	return nil
}
