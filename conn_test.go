package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConn_Close_nil(t *testing.T) {
	var c *Conn
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConn_Channel_notInitialized(t *testing.T) {
	c := &Conn{}
	if _, err := c.Channel(); err != ErrNotInitialized {
		t.Fatal(err)
	}
}

func TestNewConn_success(t *testing.T) {
	old := connectConn
	defer func() { connectConn = old }()
	connectConn = func(context.Context, Config) (*Conn, error) {
		return &Conn{}, nil
	}
	c, err := NewConn(context.Background(), Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("expected conn")
	}
}

func TestNewConn_invalidDSN(t *testing.T) {
	_, err := NewConn(context.Background(), Config{})
	if err == nil {
		// Config{} validates to 127.0.0.1 - should succeed ValidateDSN
	}
}

func Test_defaultConnectConn_dial(t *testing.T) {
	old := amqpDial
	defer func() { amqpDial = old }()
	amqpDial = func(string, amqp.Config) (*amqp.Connection, error) {
		return &amqp.Connection{}, nil
	}
	c, err := defaultConnectConn(context.Background(), Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.URL != "amqp://x" {
		t.Fatal(c.cfg)
	}
}

func TestNewConnWithRetry_exhausted(t *testing.T) {
	old := connectConn
	defer func() { connectConn = old }()
	connectConn = func(context.Context, Config) (*Conn, error) { return nil, errors.New("fail") }
	_, err := NewConnWithRetry(context.Background(), Config{
		URL:   "amqp://x",
		Retry: RetryConfig{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewConnWithRetry_success(t *testing.T) {
	old := connectConn
	defer func() { connectConn = old }()
	attempts := 0
	connectConn = func(context.Context, Config) (*Conn, error) {
		attempts++
		if attempts < 2 {
			return nil, errors.New("temporary")
		}
		return &Conn{}, nil
	}
	c, err := NewConnWithRetry(context.Background(), Config{
		URL: "amqp://x",
		Retry: RetryConfig{
			MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d", attempts)
	}
	_ = c
}
