package rabbitmq

import (
	"context"
	"net"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Test_applyAMQPConfig_defaults(t *testing.T) {
	cfg := Config{Heartbeat: 30 * time.Second, DialTimeout: 5 * time.Second}
	c := applyAMQPConfig(context.Background(), cfg)
	if c.Heartbeat != 30*time.Second || c.Dial == nil {
		t.Fatal(c)
	}
}

func Test_applyAMQPConfig_override(t *testing.T) {
	custom := &amqp.Config{Heartbeat: 1}
	cfg := Config{AMQPConfig: custom}
	if applyAMQPConfig(context.Background(), cfg) != custom {
		t.Fatal("expected override")
	}
}

func Test_applyAMQPConfig_timeoutFallback(t *testing.T) {
	c := applyAMQPConfig(nil, Config{Timeout: time.Second}) //nolint:staticcheck // nil ctx is tolerated
	if c.Dial == nil {
		t.Fatal("Config.Timeout must configure the dialer")
	}
	if got := effectiveDialTimeout(Config{Timeout: time.Second}); got != time.Second {
		t.Fatal(got)
	}
	if got := effectiveDialTimeout(Config{Timeout: time.Second, DialTimeout: time.Minute}); got != time.Minute {
		t.Fatal(got)
	}
	if c := applyAMQPConfig(context.Background(), Config{}); c.Dial != nil {
		t.Fatal("no timeout configured: driver default dial expected")
	}
}

func Test_applyAMQPConfig_dialUsesTimeoutAndCtx(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c := applyAMQPConfig(context.Background(), Config{Timeout: time.Second})
	conn, err := c.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c = applyAMQPConfig(ctx, Config{Timeout: time.Second})
	if _, err := c.Dial("tcp", ln.Addr().String()); err == nil {
		t.Fatal("expected cancelled ctx to abort dial")
	}
	if _, err := c.Dial("tcp", "bad address"); err == nil {
		t.Fatal("expected dial error")
	}
}
