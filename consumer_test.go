package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeConsumeChannel struct {
	qosErr     error
	consumeErr error
	queueErr   error
	deliveries <-chan amqp.Delivery
}

func (f *fakeConsumeChannel) Qos(int, int, bool) error { return f.qosErr }
func (f *fakeConsumeChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	return f.deliveries, nil
}
func (f *fakeConsumeChannel) QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error) {
	if f.queueErr != nil {
		return amqp.Queue{}, f.queueErr
	}
	return amqp.Queue{Name: "q"}, nil
}
func (f *fakeConsumeChannel) Close() error { return nil }

func TestConsumer_Close_nil(t *testing.T) {
	var c *consumer
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConsumer_Consume_uninitialized(t *testing.T) {
	var c *consumer
	if err := c.Consume(context.Background(), "q", nil); err != ErrNotInitialized {
		t.Fatal(err)
	}
}

func TestNewConsumer_connError(t *testing.T) {
	oldConn := newConnForConsumer
	defer func() { newConnForConsumer = oldConn }()
	newConnForConsumer = func(Config) (*Conn, error) { return nil, errors.New("conn") }
	_, err := NewConsumer(Config{URL: "amqp://x"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewConsumer_channelError(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	openConsumeChannel = func(*Conn) (consumeChannel, error) {
		return nil, errors.New("channel failed")
	}
	_, err := NewConsumer(Config{URL: "amqp://x"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestConsumer_Consume_successAck(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	ch := make(chan amqp.Delivery, 1)
	ch <- amqp.Delivery{Body: []byte("ok")}
	close(ch)
	openConsumeChannel = func(*Conn) (consumeChannel, error) {
		return &fakeConsumeChannel{deliveries: ch}, nil
	}
	c, err := NewConsumer(Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = c.Consume(ctx, "q", func(context.Context, amqp.Delivery) error { return nil })
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestConsumer_onHandlerError_default(t *testing.T) {
	c := &consumer{cfg: Config{}}
	c.onHandlerError(errors.New("x"))
}

func TestNewConsumer_success(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	openConsumeChannel = func(*Conn) (consumeChannel, error) { return &fakeConsumeChannel{}, nil }

	c, err := NewConsumer(Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureQueue("q"); err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestNewConsumer_qosError(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	openConsumeChannel = func(*Conn) (consumeChannel, error) {
		return &fakeConsumeChannel{qosErr: errors.New("qos")}, nil
	}
	c, err := NewConsumer(Config{URL: "amqp://x", PrefetchCount: 5})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = c.Consume(ctx, "q", func(context.Context, amqp.Delivery) error { return nil })
	if err == nil {
		t.Fatal("expected ctx error")
	}
}

func TestConsumer_ErrorHandler(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	ch := make(chan amqp.Delivery, 1)
	ch <- amqp.Delivery{Body: []byte("x")}
	close(ch)
	openConsumeChannel = func(*Conn) (consumeChannel, error) {
		return &fakeConsumeChannel{deliveries: ch}, nil
	}
	var handled error
	c, err := NewConsumer(Config{
		URL:          "amqp://x",
		ErrorHandler: func(err error) { handled = err },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = c.Consume(ctx, "q", func(context.Context, amqp.Delivery) error {
		return errors.New("handler fail")
	})
	if handled == nil {
		t.Fatal("expected error handler call")
	}
}

func TestNewConsumerWithRetry(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	openConsumeChannel = func(*Conn) (consumeChannel, error) { return &fakeConsumeChannel{}, nil }

	c, err := NewConsumerWithRetry(Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestConsumer_NotifyClose_nil(t *testing.T) {
	var c *consumer
	if c.NotifyClose() != nil {
		t.Fatal("expected nil")
	}
}

func TestConsumer_NotifyClose_delegates(t *testing.T) {
	old := amqpDial
	defer func() { amqpDial = old }()
	amqpDial = func(string, amqp.Config) (*amqp.Connection, error) {
		return &amqp.Connection{}, nil
	}
	conn, err := defaultConnectConn(Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	c := &consumer{conn: conn}
	if ch := c.NotifyClose(); ch == nil {
		t.Fatal("expected notify channel")
	}
}

func TestConsumer_Consume_deliveryClosed(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	ch := make(chan amqp.Delivery)
	close(ch)
	openConsumeChannel = func(*Conn) (consumeChannel, error) {
		return &fakeConsumeChannel{deliveries: ch}, nil
	}
	c, err := NewConsumer(Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = c.Consume(ctx, "q", func(context.Context, amqp.Delivery) error { return nil })
}

func TestConsumer_EnsureQueue_error(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	openConsumeChannel = func(*Conn) (consumeChannel, error) {
		return &fakeConsumeChannel{queueErr: errors.New("queue")}, nil
	}
	c, err := NewConsumer(Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureQueue("q"); err == nil {
		t.Fatal("expected error")
	}
}

func TestConsumer_Consume_consumeError(t *testing.T) {
	oldConn := newConnForConsumer
	oldOpen := openConsumeChannel
	defer func() {
		newConnForConsumer = oldConn
		openConsumeChannel = oldOpen
	}()
	newConnForConsumer = func(Config) (*Conn, error) { return &Conn{}, nil }
	openConsumeChannel = func(*Conn) (consumeChannel, error) {
		return &fakeConsumeChannel{consumeErr: errors.New("consume")}, nil
	}
	c, err := NewConsumer(Config{URL: "amqp://x"})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Consume(context.Background(), "q", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
