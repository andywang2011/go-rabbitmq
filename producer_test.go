package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakePublishChannel struct {
	published int
	closeErr  error
	pubErr    error
}

func (f *fakePublishChannel) PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error {
	if f.pubErr != nil {
		return f.pubErr
	}
	f.published++
	return nil
}
func (f *fakePublishChannel) Close() error { return f.closeErr }

func TestProducer_Close_nil(t *testing.T) {
	var p *producer
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProducer_Publish_uninitialized(t *testing.T) {
	var p *producer
	if err := p.Publish(context.Background(), "", "", nil); err != ErrNotInitialized {
		t.Fatal(err)
	}
}

func TestNewProducer_success(t *testing.T) {
	oldConn := newConnForProducer
	oldOpen := openPublishChannel
	defer func() {
		newConnForProducer = oldConn
		openPublishChannel = oldOpen
	}()
	newConnForProducer = func(context.Context, Config) (*Conn, error) { return &Conn{}, nil }
	fake := &fakePublishChannel{}
	openPublishChannel = func(*Conn) (publishChannel, error) { return fake, nil }

	p, err := NewProducer(context.Background(), Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), "", "q", []byte("hi"), WithJSON()); err != nil {
		t.Fatal(err)
	}
	if fake.published != 1 {
		t.Fatal(fake.published)
	}
	p.Close()
}

func TestNewProducer_channelError(t *testing.T) {
	oldConn := newConnForProducer
	oldOpen := openPublishChannel
	defer func() {
		newConnForProducer = oldConn
		openPublishChannel = oldOpen
	}()
	newConnForProducer = func(context.Context, Config) (*Conn, error) { return &Conn{}, nil }
	openPublishChannel = func(*Conn) (publishChannel, error) {
		return nil, errors.New("channel failed")
	}
	_, err := NewProducer(context.Background(), Config{URL: "amqp://x"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestProducer_Publish_error(t *testing.T) {
	p := &producer{channel: &fakePublishChannel{pubErr: errors.New("pub")}}
	if err := p.Publish(context.Background(), "", "", nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestProducer_Close_withResources(t *testing.T) {
	p := &producer{
		channel: &fakePublishChannel{closeErr: errors.New("ch")},
		conn:    &Conn{},
	}
	_ = p.Close()
}

func TestNewProducer_connError(t *testing.T) {
	oldConn := newConnForProducer
	defer func() { newConnForProducer = oldConn }()
	newConnForProducer = func(context.Context, Config) (*Conn, error) { return nil, errors.New("conn") }
	_, err := NewProducer(context.Background(), Config{URL: "amqp://x"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewProducerWithRetry_exhausted(t *testing.T) {
	oldConn := newConnForProducer
	defer func() { newConnForProducer = oldConn }()
	newConnForProducer = func(context.Context, Config) (*Conn, error) { return nil, errors.New("fail") }
	_, err := NewProducerWithRetry(context.Background(), Config{
		URL:   "amqp://x",
		Retry: RetryConfig{MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewProducerWithRetry_success(t *testing.T) {
	oldConn := newConnForProducer
	oldOpen := openPublishChannel
	defer func() {
		newConnForProducer = oldConn
		openPublishChannel = oldOpen
	}()
	attempts := 0
	newConnForProducer = func(context.Context, Config) (*Conn, error) {
		attempts++
		if attempts < 2 {
			return nil, errors.New("temporary")
		}
		return &Conn{}, nil
	}
	openPublishChannel = func(*Conn) (publishChannel, error) { return &fakePublishChannel{}, nil }

	p, err := NewProducerWithRetry(context.Background(), Config{
		URL: "amqp://x",
		Retry: RetryConfig{
			MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
}
