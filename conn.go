package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Conn wraps an AMQP connection.
type Conn struct {
	conn *amqp.Connection
	cfg  Config
}

var connectConn = defaultConnectConn

func defaultConnectConn(cfg Config) (*Conn, error) {
	if err := cfg.ValidateDSN(); err != nil {
		return nil, err
	}
	amqpConn, err := dialConn(cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: amqpConn, cfg: cfg}, nil
}

// NewConn creates a connection from config (first successful URL when URLs is set).
func NewConn(cfg Config) (*Conn, error) {
	return connectConn(cfg)
}

// NewConnWithRetry creates a connection with exponential-backoff startup retry.
func NewConnWithRetry(cfg Config) (*Conn, error) {
	if err := cfg.ValidateDSN(); err != nil {
		return nil, err
	}
	retry := cfg.Retry
	if retry.MaxAttempts <= 0 {
		retry = DefaultRetryConfig()
	}
	normalizeRetry(&retry)
	rm := newRetryManager(retry)
	var conn *Conn
	var lastErr error
	err := rm.execute(context.Background(), func(context.Context) error {
		c, err := connectConn(cfg)
		if err != nil {
			lastErr = err
			return err
		}
		conn = c
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("rabbitmq connect retry failed after %d attempts: %w", retry.MaxAttempts, lastErr)
	}
	return conn, nil
}

// Channel opens a new channel; caller must Close it.
func (c *Conn) Channel() (*amqp.Channel, error) {
	if c == nil || c.conn == nil {
		return nil, ErrNotInitialized
	}
	return c.conn.Channel()
}

// NotifyClose registers a listener for connection close notifications.
func (c *Conn) NotifyClose() <-chan *amqp.Error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.NotifyClose(make(chan *amqp.Error, 1))
}

// Close closes the connection; safe to call multiple times.
func (c *Conn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
