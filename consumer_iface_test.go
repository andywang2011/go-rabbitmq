package rabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestWrapConsumer_nil(t *testing.T) {
	if WrapConsumer(nil) != nil {
		t.Fatal("expected nil")
	}
}

func TestWrapConsumer_close(t *testing.T) {
	f := &fakeConsumer{}
	h := WrapConsumer(f)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}

type notifyConsumer struct {
	fakeConsumer
	ch chan *amqp.Error
}

func (n *notifyConsumer) NotifyClose() <-chan *amqp.Error { return n.ch }

func TestWrapConsumer_notifyClose(t *testing.T) {
	ch := make(chan *amqp.Error, 1)
	h := WrapConsumer(&notifyConsumer{ch: ch})
	if h.NotifyClose() != ch {
		t.Fatal("expected channel")
	}
}
