package rabbitmq

import (
	"fmt"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Config holds RabbitMQ connection settings.
type Config struct {
	URL      string
	URLs     []string
	Host     string
	Port     int
	User     string
	Password string
	Vhost    string
	Timeout  time.Duration
	// Heartbeat and DialTimeout map to amqp.Config when AMQPConfig is nil.
	Heartbeat   time.Duration
	DialTimeout time.Duration
	// PrefetchCount is applied via Channel.Qos before consume (default 1).
	PrefetchCount  int
	PrefetchGlobal bool
	// Retry optional startup connect retry (dial + channel open only).
	Retry RetryConfig
	// AMQPConfig optional full override of driver config (TLS, SASL, etc.).
	AMQPConfig *amqp.Config
	// ErrorHandler optional callback when a message handler returns an error.
	ErrorHandler func(error)
}

const (
	defaultTimeout       = 10 * time.Second
	defaultPrefetchCount = 1
	defaultPort          = 5672
)

// ConfigDefault returns production-oriented defaults (URL/Host must still be set).
func ConfigDefault() Config {
	return Config{
		Timeout:       defaultTimeout,
		PrefetchCount: defaultPrefetchCount,
		Retry:         DefaultRetryConfig(),
	}
}

// DefaultRetryConfig returns startup retry defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		Multiplier:   2.0,
	}
}

// Validate fills defaults without dialing the broker.
func (c *Config) Validate() error {
	if c.Timeout == 0 {
		c.Timeout = defaultTimeout
	}
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.Host == "" && c.URL == "" && len(c.URLs) == 0 {
		c.Host = "127.0.0.1"
	}
	if c.PrefetchCount < 1 {
		c.PrefetchCount = defaultPrefetchCount
	}
	if c.Retry.MaxAttempts > 0 {
		normalizeRetry(&c.Retry)
	}
	return nil
}

// DSN returns an amqp URL; URL field takes precedence over host parts.
func (c Config) DSN() string {
	if c.URL != "" {
		return c.URL
	}
	user := c.User
	if user == "" {
		user = "guest"
	}
	pass := c.Password
	if pass == "" {
		pass = "guest"
	}
	vhost := c.Vhost
	path := "%2F"
	if vhost != "" && vhost != "/" {
		path = url.PathEscape(vhost)
	}
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", user, pass, c.Host, c.Port, path)
}

// ValidateDSN ensures at least one dial URL can be built.
func (c Config) ValidateDSN() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if len(dsnList(c)) == 0 {
		return ErrNoURL
	}
	return nil
}
