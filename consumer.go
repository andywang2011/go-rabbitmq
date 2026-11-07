package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageHandler is invoked per delivery; return nil to ack.
type MessageHandler func(ctx context.Context, msg amqp.Delivery) error

type consumer struct {
	conn    *Conn
	channel consumeChannel
	cfg     Config
}

var _ Consumer = (*consumer)(nil)

var newConnForConsumer = NewConn

func newConsumerFromConn(ctx context.Context, cfg Config, connect func(context.Context, Config) (*Conn, error)) (Consumer, error) {
	cfg, err := prepareConfig(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	ch, err := openConsumeChannel(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	return &consumer{conn: conn, channel: ch, cfg: cfg}, nil
}

// NewConsumer creates a consumer (returns mock-friendly Consumer interface).
// The config is validated before connecting; ctx bounds the connect.
func NewConsumer(ctx context.Context, cfg Config) (Consumer, error) {
	cfg, err := prepareConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newConsumerFromConn(ctxOrBackground(ctx), cfg, newConnForConsumer)
}

// NewConsumerWithRetry creates a consumer with exponential-backoff startup retry.
// Retries stop as soon as ctx is cancelled.
func NewConsumerWithRetry(ctx context.Context, cfg Config) (Consumer, error) {
	ctx = ctxOrBackground(ctx)
	cfg, err := prepareConfig(cfg)
	if err != nil {
		return nil, err
	}
	retry := cfg.Retry
	if retry.MaxAttempts <= 0 {
		retry = DefaultRetryConfig()
	}
	normalizeRetry(&retry)
	cfg.Retry = retry
	rm := newRetryManager(retry)
	var consumer Consumer
	var lastErr error
	err = rm.execute(ctx, func(ctx context.Context) error {
		c, err := NewConsumer(ctx, cfg)
		if err != nil {
			lastErr = err
			return err
		}
		consumer = c
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("rabbitmq consumer retry failed after %d attempts: %w", retry.MaxAttempts, lastErr)
	}
	return consumer, nil
}

// shouldRequeue reports whether a failed delivery may be requeued. When
// MaxRedelivery is exceeded it returns false so the broker dead-letters it.
func (c *consumer) shouldRequeue(d amqp.Delivery) bool {
	if c.cfg.MaxRedelivery <= 0 {
		return true
	}
	return deliveryCount(d) < c.cfg.MaxRedelivery
}

// deliveryCount returns how many times d was already delivered before this
// attempt. Quorum queues expose the exact value in x-delivery-count; otherwise
// the redelivered flag yields a count of one.
func deliveryCount(d amqp.Delivery) int {
	switch v := d.Headers["x-delivery-count"].(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	}
	if d.Redelivered {
		return 1
	}
	return 0
}

func (c *consumer) onHandlerError(err error) {
	if c.cfg.ErrorHandler != nil {
		c.cfg.ErrorHandler(err)
		return
	}
	log.Printf("rabbitmq handler err: %v", err)
}

// Consume consumes queue sequentially until ctx is cancelled.
func (c *consumer) Consume(ctx context.Context, queue string, handler MessageHandler) error {
	return c.ConsumeWithConcurrency(ctx, queue, 1, handler)
}

// ConsumeWithConcurrency consumes queue with worker goroutines.
func (c *consumer) ConsumeWithConcurrency(ctx context.Context, queue string, concurrency int, handler MessageHandler) error {
	if c == nil || c.channel == nil {
		return ErrNotInitialized
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if err := c.channel.Qos(c.cfg.PrefetchCount, 0, c.cfg.PrefetchGlobal); err != nil {
		return fmt.Errorf("rabbitmq qos: %w", err)
	}
	deliveries, err := c.channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq consume: %w", err)
	}

	msgCh := make(chan amqp.Delivery, concurrency*2)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range msgCh {
				if err := handler(ctx, d); err != nil {
					_ = d.Nack(false, c.shouldRequeue(d))
					c.onHandlerError(err)
				} else {
					_ = d.Ack(false)
				}
			}
		}()
	}

	var loopErr error
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			goto done
		case d, ok := <-deliveries:
			if !ok {
				goto done
			}
			select {
			case msgCh <- d:
			case <-ctx.Done():
				// Hand the unprocessed delivery back so it is not stuck unacked.
				_ = d.Nack(false, true)
				loopErr = ctx.Err()
				goto done
			}
		}
	}
done:
	close(msgCh)
	wg.Wait()
	return loopErr
}

// EnsureQueue declares a durable queue.
func (c *consumer) EnsureQueue(name string) error {
	if c == nil || c.channel == nil {
		return ErrNotInitialized
	}
	_, err := c.channel.QueueDeclare(name, true, false, false, false, nil)
	return err
}

// Close closes channel and connection.
func (c *consumer) Close() error {
	if c == nil {
		return nil
	}
	var err error
	if c.channel != nil {
		err = c.channel.Close()
	}
	if c.conn != nil {
		if e := c.conn.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// NotifyClose exposes connection close notifications for reconnect logic.
func (c *consumer) NotifyClose() <-chan *amqp.Error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.NotifyClose()
}
