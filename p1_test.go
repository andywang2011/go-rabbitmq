package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeAcker struct {
	nacks chan bool // requeue flag per Nack
	acks  chan struct{}
}

func newFakeAcker() *fakeAcker {
	return &fakeAcker{nacks: make(chan bool, 8), acks: make(chan struct{}, 8)}
}

func (f *fakeAcker) Ack(uint64, bool) error { f.acks <- struct{}{}; return nil }
func (f *fakeAcker) Nack(_ uint64, _ bool, requeue bool) error {
	f.nacks <- requeue
	return nil
}
func (f *fakeAcker) Reject(uint64, bool) error { return nil }

func TestConfig_Validate_rejectsInvalid(t *testing.T) {
	cases := map[string]Config{
		"neg timeout":    {Timeout: -1},
		"neg heartbeat":  {Heartbeat: -1},
		"neg dial":       {DialTimeout: -1},
		"port high":      {Port: 70000},
		"port neg":       {Port: -1},
		"neg prefetch":   {PrefetchCount: -1},
		"neg redelivery": {MaxRedelivery: -1},
		"bad scheme":     {URL: "http://x"},
		"bad url":        {URL: "amqp://%zz"},
		"no host":        {URLs: []string{"amqp://"}},
	}
	for name, cfg := range cases {
		if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s: got %v", name, err)
		}
	}
	ok := Config{URLs: []string{"amqps://h:5671/v"}}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareConfig(t *testing.T) {
	if _, err := prepareConfig(Config{Timeout: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	cfg, err := prepareConfig(Config{})
	if err != nil || cfg.Host == "" {
		t.Fatal(cfg, err)
	}
}

func TestNew_validatesBeforeConnect(t *testing.T) {
	oldC, oldP, oldN := newConnForConsumer, newConnForProducer, connectConn
	defer func() { newConnForConsumer, newConnForProducer, connectConn = oldC, oldP, oldN }()
	called := false
	fn := func(context.Context, Config) (*Conn, error) { called = true; return &Conn{}, nil }
	newConnForConsumer, newConnForProducer, connectConn = fn, fn, fn
	bad := Config{Port: -5}
	ctx := context.Background()
	if _, err := NewConsumer(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewProducer(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewConn(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewConnWithRetry(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewConsumerWithRetry(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewProducerWithRetry(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if called {
		t.Fatal("connect must not run for invalid config")
	}
}

func TestNew_nilCtxTolerated(t *testing.T) {
	old := connectConn
	defer func() { connectConn = old }()
	connectConn = func(ctx context.Context, _ Config) (*Conn, error) {
		if ctx == nil {
			t.Fatal("ctx must be normalised")
		}
		return &Conn{}, nil
	}
	//lint:ignore SA1012 intentional nil context in test
	if _, err := NewConn(nil, Config{URL: "amqp://x"}); err != nil {
		t.Fatal(err)
	}
}

func TestDeliveryCount(t *testing.T) {
	cases := []struct {
		d    amqp.Delivery
		want int
	}{
		{amqp.Delivery{}, 0},
		{amqp.Delivery{Redelivered: true}, 1},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int(2)}}, 2},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int8(3)}}, 3},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int16(4)}}, 4},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int32(5)}}, 5},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int64(6)}}, 6},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": uint8(7)}}, 7},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": uint16(8)}}, 8},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": uint32(9)}}, 9},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": uint64(10)}}, 10},
		{amqp.Delivery{Headers: amqp.Table{"x-delivery-count": "bad"}}, 0},
	}
	for i, tc := range cases {
		if got := deliveryCount(tc.d); got != tc.want {
			t.Fatalf("case %d: got %d want %d", i, got, tc.want)
		}
	}
}

func TestConsumer_shouldRequeue(t *testing.T) {
	c := &consumer{}
	if !c.shouldRequeue(amqp.Delivery{Redelivered: true}) {
		t.Fatal("unlimited by default")
	}
	c.cfg.MaxRedelivery = 2
	if !c.shouldRequeue(amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int64(1)}}) {
		t.Fatal("below limit must requeue")
	}
	if c.shouldRequeue(amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int64(2)}}) {
		t.Fatal("limit reached must dead-letter")
	}
}

func runConsume(t *testing.T, cfg Config, d amqp.Delivery, handlerErr error) (nack, requeued bool) {
	t.Helper()
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- d
	c := &consumer{channel: &fakeConsumeChannel{deliveries: deliveries}, cfg: cfg}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- c.Consume(ctx, "q", func(context.Context, amqp.Delivery) error { return handlerErr })
	}()
	acker := d.Acknowledger.(*fakeAcker)
	select {
	case requeue := <-acker.nacks:
		nack, requeued = true, requeue
	case <-acker.acks:
	case <-time.After(time.Second):
		t.Fatal("no ack/nack")
	}
	cancel()
	<-done
	return nack, requeued
}

func TestConsumer_MaxRedelivery_deadLetters(t *testing.T) {
	a := newFakeAcker()
	d := amqp.Delivery{Acknowledger: a, Redelivered: true, Headers: amqp.Table{"x-delivery-count": int64(3)}}
	if nack, requeue := runConsume(t, Config{MaxRedelivery: 3, ErrorHandler: func(error) {}}, d, errors.New("boom")); !nack || requeue {
		t.Fatal("expected requeue=false once MaxRedelivery exceeded")
	}
}

func TestConsumer_MaxRedelivery_requeuesBelowLimit(t *testing.T) {
	a := newFakeAcker()
	d := amqp.Delivery{Acknowledger: a}
	if nack, requeue := runConsume(t, Config{MaxRedelivery: 3, ErrorHandler: func(error) {}}, d, errors.New("boom")); !nack || !requeue {
		t.Fatal("expected requeue=true below limit")
	}
}

func TestConsumer_ctxCancelWhileWorkersBusy_doesNotHang(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 16)
	a := newFakeAcker()
	for i := 0; i < 8; i++ {
		deliveries <- amqp.Delivery{Acknowledger: a}
	}
	c := &consumer{channel: &fakeConsumeChannel{deliveries: deliveries}, cfg: Config{PrefetchCount: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- c.ConsumeWithConcurrency(ctx, "q", 1, func(context.Context, amqp.Delivery) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil
		})
	}()
	<-started
	time.Sleep(50 * time.Millisecond) // let msgCh fill so the loop blocks on send
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consume hung after ctx cancel")
	}
}

func TestRetry_ctxCancelDuringBackoff(t *testing.T) {
	rm := newRetryManager(RetryConfig{MaxAttempts: 5, InitialDelay: time.Hour, MaxDelay: time.Hour, Multiplier: 1})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	err := rm.execute(ctx, func(context.Context) error { return errors.New("x") })
	if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
		t.Fatal(err, time.Since(start))
	}
}

func TestRetry_ctxCancelDuringOp(t *testing.T) {
	rm := newRetryManager(DefaultRetryConfig())
	ctx, cancel := context.WithCancel(context.Background())
	err := rm.execute(ctx, func(context.Context) error { cancel(); return errors.New("x") })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRetry_nilCtxAndZeroSleep(t *testing.T) {
	rm := newRetryManager(DefaultRetryConfig())
	//lint:ignore SA1012 intentional nil context in test
	if err := rm.execute(nil, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := rm.sleep(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
}

func TestNewConnWithRetry_ctxCancelled(t *testing.T) {
	old := connectConn
	defer func() { connectConn = old }()
	connectConn = func(context.Context, Config) (*Conn, error) { return nil, errors.New("fail") }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewConnWithRetry(ctx, Config{URL: "amqp://x"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestDefaultConnectConn_ctxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := defaultConnectConn(ctx, Config{URL: "amqp://x"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := defaultConnectConn(context.Background(), Config{Port: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
}

func TestDialConn_ctxCancelledBetweenURLs(t *testing.T) {
	old := amqpDial
	defer func() { amqpDial = old }()
	ctx, cancel := context.WithCancel(context.Background())
	amqpDial = func(string, amqp.Config) (*amqp.Connection, error) {
		cancel()
		return nil, errors.New("down")
	}
	_, err := dialConn(ctx, Config{URLs: []string{"amqp://a", "amqp://b"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
