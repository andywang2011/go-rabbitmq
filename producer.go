package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type producer struct {
	conn    *Conn
	channel publishChannel
}

var _ Producer = (*producer)(nil)

var openConsumeChannel = func(c *Conn) (consumeChannel, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

var (
	newConnForProducer = NewConn
	openPublishChannel = func(c *Conn) (publishChannel, error) {
		ch, err := c.Channel()
		if err != nil {
			return nil, err
		}
		return ch, nil
	}
)

func newProducerFromConn(cfg Config, connect func(Config) (*Conn, error)) (Producer, error) {
	conn, err := connect(cfg)
	if err != nil {
		return nil, err
	}
	ch, err := openPublishChannel(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	return &producer{conn: conn, channel: ch}, nil
}

// NewProducer creates a producer (returns mock-friendly Producer interface).
func NewProducer(cfg Config) (Producer, error) {
	if err := cfg.ValidateDSN(); err != nil {
		return nil, err
	}
	return newProducerFromConn(cfg, newConnForProducer)
}

// NewProducerWithRetry creates a producer with exponential-backoff startup retry.
func NewProducerWithRetry(cfg Config) (Producer, error) {
	if err := cfg.ValidateDSN(); err != nil {
		return nil, err
	}
	retry := cfg.Retry
	if retry.MaxAttempts <= 0 {
		retry = DefaultRetryConfig()
	}
	normalizeRetry(&retry)
	cfg.Retry = retry
	rm := newRetryManager(retry)
	var producer Producer
	var lastErr error
	err := rm.execute(context.Background(), func(context.Context) error {
		p, err := NewProducer(cfg)
		if err != nil {
			lastErr = err
			return err
		}
		producer = p
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("rabbitmq producer retry failed after %d attempts: %w", retry.MaxAttempts, lastErr)
	}
	return producer, nil
}

// Publish publishes a message.
func (p *producer) Publish(ctx context.Context, exchange, routingKey string, body []byte, opts ...PublishOption) error {
	if p == nil || p.channel == nil {
		return ErrNotInitialized
	}
	if ctx == nil {
		ctx = context.Background()
	}
	msg := amqp.Publishing{
		ContentType:  "application/octet-stream",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}
	for _, o := range opts {
		o(&msg)
	}
	return p.channel.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
}

// Close closes the channel and connection.
func (p *producer) Close() error {
	if p == nil {
		return nil
	}
	var err error
	if p.channel != nil {
		err = p.channel.Close()
	}
	if p.conn != nil {
		if e := p.conn.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// PublishOption configures a published message.
type PublishOption func(*amqp.Publishing)

// WithContentType sets Content-Type.
func WithContentType(ct string) PublishOption {
	return func(p *amqp.Publishing) { p.ContentType = ct }
}

// WithJSON sets application/json.
func WithJSON() PublishOption {
	return WithContentType("application/json")
}
