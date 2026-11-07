package rabbitmq

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Config holds RabbitMQ connection settings.
type Config struct {
	// URL is a full amqp:// connection string (highest precedence).
	URL string
	// URLs is a failover list of amqp:// connection strings.
	URLs []string
	// Host is the broker hostname when URL is empty.
	Host string
	// Port is the broker port (default 5672).
	Port int
	// User is the login name (default guest).
	User string
	// Password is the login password (default guest).
	Password string
	// Vhost is the virtual host path.
	Vhost string
	// Timeout bounds overall connect operations. It is also used as the TCP
	// dial and handshake timeout when DialTimeout is zero and AMQPConfig is nil.
	Timeout time.Duration
	// Heartbeat is the AMQP heartbeat interval when AMQPConfig is nil.
	Heartbeat time.Duration
	// DialTimeout bounds the TCP dial when AMQPConfig is nil (default: Timeout).
	DialTimeout time.Duration
	// PrefetchCount is applied via Channel.Qos before consume (default 1).
	PrefetchCount int
	// PrefetchGlobal sets the global Qos flag when applying PrefetchCount.
	PrefetchGlobal bool
	// MaxRedelivery is the maximum number of redeliveries allowed for a message
	// whose handler keeps failing. Once exceeded the message is rejected with
	// requeue=false so the broker can route it to a dead-letter exchange.
	// Zero means unlimited redelivery. Exact counting uses the x-delivery-count
	// header (quorum queues); on other queues only the redelivered flag is known.
	MaxRedelivery int
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

// Validate checks the configuration and fills defaults without dialing the broker.
func (c *Config) Validate() error {
	if c.Timeout < 0 || c.Heartbeat < 0 || c.DialTimeout < 0 {
		return fmt.Errorf("%w: durations must not be negative", ErrInvalidConfig)
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("%w: port %d out of range", ErrInvalidConfig, c.Port)
	}
	if c.PrefetchCount < 0 {
		return fmt.Errorf("%w: prefetch count must not be negative", ErrInvalidConfig)
	}
	if c.MaxRedelivery < 0 {
		return fmt.Errorf("%w: max redelivery must not be negative", ErrInvalidConfig)
	}
	for _, raw := range append([]string{c.URL}, c.URLs...) {
		if raw == "" {
			continue
		}
		if err := validateURL(raw); err != nil {
			return err
		}
	}
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

func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: parse url: %v", ErrInvalidConfig, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "amqp" && scheme != "amqps" {
		return fmt.Errorf("%w: unsupported url scheme %q", ErrInvalidConfig, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: url has no host", ErrInvalidConfig)
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
