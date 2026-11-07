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

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// prepareConfig validates cfg, applies defaults and ensures a dial URL exists.
// It never touches the network.
func prepareConfig(cfg Config) (Config, error) {
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	if len(dsnList(cfg)) == 0 {
		return cfg, ErrNoURL
	}
	return cfg, nil
}

func defaultConnectConn(ctx context.Context, cfg Config) (*Conn, error) {
	ctx = ctxOrBackground(ctx)
	cfg, err := prepareConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	amqpConn, err := dialConn(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: amqpConn, cfg: cfg}, nil
}

// NewConn creates a connection from config (first successful URL when URLs is set).
// The config is validated before any network I/O; ctx cancels the TCP dial.
func NewConn(ctx context.Context, cfg Config) (*Conn, error) {
	cfg, err := prepareConfig(cfg)
	if err != nil {
		return nil, err
	}
	return connectConn(ctxOrBackground(ctx), cfg)
}

// NewConnWithRetry creates a connection with exponential-backoff startup retry.
// Retries stop as soon as ctx is cancelled.
func NewConnWithRetry(ctx context.Context, cfg Config) (*Conn, error) {
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
	rm := newRetryManager(retry)
	var conn *Conn
	var lastErr error
	err = rm.execute(ctx, func(ctx context.Context) error {
		c, err := connectConn(ctx, cfg)
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
