package rabbitmq

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Test_dialConn_failover(t *testing.T) {
	old := amqpDial
	defer func() { amqpDial = old }()
	var calls []string
	amqpDial = func(url string, _ amqp.Config) (*amqp.Connection, error) {
		calls = append(calls, url)
		if url == "amqp://second" {
			return &amqp.Connection{}, nil
		}
		return nil, errors.New("dial failed")
	}
	_, err := dialConn(Config{URLs: []string{"amqp://first", "amqp://second"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[1] != "amqp://second" {
		t.Fatalf("calls=%v", calls)
	}
}

func Test_dialConn_allFail(t *testing.T) {
	old := amqpDial
	defer func() { amqpDial = old }()
	amqpDial = func(string, amqp.Config) (*amqp.Connection, error) {
		return nil, errors.New("fail")
	}
	_, err := dialConn(Config{URL: "amqp://x"})
	if err == nil {
		t.Fatal("expected error")
	}
}
