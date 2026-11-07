package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func Test_normalizeRetry(t *testing.T) {
	r := RetryConfig{}
	normalizeRetry(&r)
	if r.MaxAttempts != DefaultRetryConfig().MaxAttempts {
		t.Fatal(r)
	}
}

func Test_retryManager_execute_success(t *testing.T) {
	rm := newRetryManager(DefaultRetryConfig())
	if err := rm.execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func Test_retryManager_execute_exhausted(t *testing.T) {
	rm := newRetryManager(RetryConfig{
		MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1,
	})
	err := rm.execute(context.Background(), func(context.Context) error { return errors.New("x") })
	if err == nil {
		t.Fatal("expected error")
	}
}

func Test_retryManager_contextCancel(t *testing.T) {
	rm := newRetryManager(DefaultRetryConfig())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := rm.execute(ctx, func(context.Context) error { return errors.New("x") })
	if err == nil {
		t.Fatal("expected error")
	}
}

func Test_retryManager_jitter(t *testing.T) {
	rm := newRetryManager(DefaultRetryConfig())
	if d := rm.jitter(time.Millisecond); d < 0 {
		t.Fatal(d)
	}
	if d := rm.jitter(0); d != 0 {
		t.Fatal(d)
	}
}
