package rabbitmq

import (
	"net"

	amqp "github.com/rabbitmq/amqp091-go"
)

func applyAMQPConfig(cfg Config) *amqp.Config {
	if cfg.AMQPConfig != nil {
		return cfg.AMQPConfig
	}
	c := &amqp.Config{}
	if cfg.Heartbeat > 0 {
		c.Heartbeat = cfg.Heartbeat
	}
	if cfg.DialTimeout > 0 {
		timeout := cfg.DialTimeout
		c.Dial = func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, timeout)
		}
	}
	return c
}
