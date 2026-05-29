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

func newConsumerFromConn(cfg Config, connect func(Config) (*Conn, error)) (Consumer, error) {
	conn, err := connect(cfg)
	if err != nil {
		return nil, err
	}
	ch, err := openConsumeChannel(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	return &consumer{conn: conn, channel: ch, cfg: cfg}, nil
}

// NewConsumer creates a consumer (returns mock-friendly Consumer interface).
func NewConsumer(cfg Config) (Consumer, error) {
	if err := cfg.ValidateDSN(); err != nil {
		return nil, err
	}
	return newConsumerFromConn(cfg, newConnForConsumer)
}

// NewConsumerWithRetry creates a consumer with exponential-backoff startup retry.
func NewConsumerWithRetry(cfg Config) (Consumer, error) {
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
	var consumer Consumer
	var lastErr error
	err := rm.execute(context.Background(), func(context.Context) error {
		c, err := NewConsumer(cfg)
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
					_ = d.Nack(false, true)
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
			msgCh <- d
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
