package rabbitmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Test_applyAMQPConfig_defaults(t *testing.T) {
	cfg := Config{Heartbeat: 30 * time.Second, DialTimeout: 5 * time.Second}
	c := applyAMQPConfig(cfg)
	if c.Heartbeat != 30*time.Second || c.Dial == nil {
		t.Fatal(c)
	}
}

func Test_applyAMQPConfig_override(t *testing.T) {
	custom := &amqp.Config{Heartbeat: 1}
	cfg := Config{AMQPConfig: custom}
	if applyAMQPConfig(cfg) != custom {
		t.Fatal("expected override")
	}
}
