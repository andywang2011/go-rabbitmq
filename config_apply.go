package rabbitmq

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// effectiveDialTimeout returns the TCP dial and handshake timeout for cfg.
func effectiveDialTimeout(cfg Config) time.Duration {
	if cfg.DialTimeout > 0 {
		return cfg.DialTimeout
	}
	return cfg.Timeout
}

func applyAMQPConfig(ctx context.Context, cfg Config) *amqp.Config {
	if cfg.AMQPConfig != nil {
		return cfg.AMQPConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c := &amqp.Config{}
	if cfg.Heartbeat > 0 {
		c.Heartbeat = cfg.Heartbeat
	}
	if timeout := effectiveDialTimeout(cfg); timeout > 0 {
		c.Dial = func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: timeout}
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			// Bound TLS/AMQP handshake; the driver clears the deadline once open.
			if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		}
	}
	return c
}
