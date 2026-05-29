package rabbitmq

import "errors"

var (
	// ErrInvalidConfig indicates configuration validation failed.
	ErrInvalidConfig = errors.New("rabbitmq: invalid config")
	// ErrNotInitialized indicates a producer or consumer was not created.
	ErrNotInitialized = errors.New("rabbitmq: not initialized")
	// ErrNoURL indicates no AMQP URL could be built from config.
	ErrNoURL = errors.New("rabbitmq: no url")
)
