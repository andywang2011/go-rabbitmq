package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

var amqpDial = func(url string, cfg amqp.Config) (*amqp.Connection, error) {
	return amqp.DialConfig(url, cfg)
}

func dsnList(c Config) []string {
	if len(c.URLs) > 0 {
		return c.URLs
	}
	if c.URL != "" {
		return []string{c.URL}
	}
	_ = c.Validate()
	dsn := c.DSN()
	if dsn == "" {
		return nil
	}
	return []string{dsn}
}

func dialConn(cfg Config) (*amqp.Connection, error) {
	urls := dsnList(cfg)
	if len(urls) == 0 {
		return nil, ErrNoURL
	}
	amqpCfg := applyAMQPConfig(cfg)
	var lastErr error
	for _, url := range urls {
		conn, err := amqpDial(url, *amqpCfg)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("rabbitmq dial: %w", lastErr)
}
