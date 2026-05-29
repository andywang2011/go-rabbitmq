package rabbitmq

import (
	"testing"
	"time"
)

func TestConfig_DSN(t *testing.T) {
	cfg := Config{
		Host: "127.0.0.1", Port: 5672,
		User: "guest", Password: "guest", Vhost: "/",
		Timeout: 5 * time.Second,
	}
	if err := cfg.ValidateDSN(); err != nil {
		t.Fatal(err)
	}
	if cfg.DSN() == "" {
		t.Fatal("empty dsn")
	}
}

func TestConfig_DSN_urlPriority(t *testing.T) {
	cfg := Config{URL: "amqp://custom:5672/vhost"}
	if cfg.DSN() != "amqp://custom:5672/vhost" {
		t.Fatal(cfg.DSN())
	}
}

func TestConfig_Validate_defaults(t *testing.T) {
	cfg := Config{}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 5672 || cfg.Host != "127.0.0.1" || cfg.PrefetchCount != 1 {
		t.Fatalf("%+v", cfg)
	}
}

func TestConfig_URLs_dsnList(t *testing.T) {
	cfg := Config{URLs: []string{"amqp://a", "amqp://b"}}
	list := dsnList(cfg)
	if len(list) != 2 {
		t.Fatal(list)
	}
}

func TestConfigDefault(t *testing.T) {
	c := ConfigDefault()
	if c.PrefetchCount != defaultPrefetchCount {
		t.Fatal(c.PrefetchCount)
	}
}

func TestConfig_ValidateDSN_success(t *testing.T) {
	if err := (Config{URL: "amqp://x"}).ValidateDSN(); err != nil {
		t.Fatal(err)
	}
}

func TestConfig_Validate_retry(t *testing.T) {
	cfg := Config{
		Host: "h", Port: 5672,
		Retry: RetryConfig{MaxAttempts: 2, InitialDelay: time.Millisecond},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}
