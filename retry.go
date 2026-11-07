package rabbitmq

import (
	"context"
	"math/rand/v2"
	"time"
)

// RetryConfig controls exponential-backoff retry during startup connect only.
type RetryConfig struct {
	// MaxAttempts is the total number of attempts (including the first).
	MaxAttempts int
	// InitialDelay is the first backoff pause.
	InitialDelay time.Duration
	// MaxDelay caps exponential backoff.
	MaxDelay time.Duration
	// Multiplier scales delay after each failed attempt.
	Multiplier float64
}

type retryManager struct {
	config RetryConfig
	sleep  func(ctx context.Context, d time.Duration) error
}

func newRetryManager(cfg RetryConfig) *retryManager {
	return &retryManager{
		config: cfg,
		sleep: func(ctx context.Context, d time.Duration) error {
			if d <= 0 {
				return ctx.Err()
			}
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
}

func (rm *retryManager) execute(ctx context.Context, op func(ctx context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	delay := rm.config.InitialDelay
	for attempt := 0; attempt < rm.config.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := op(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < rm.config.MaxAttempts-1 {
			sleep := rm.jitter(delay)
			if err := rm.sleep(ctx, sleep); err != nil {
				return err
			}
			delay = time.Duration(float64(delay) * rm.config.Multiplier)
			if delay > rm.config.MaxDelay {
				delay = rm.config.MaxDelay
			}
		}
	}
	return lastErr
}

func (rm *retryManager) jitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	jitterRange := delay / 5
	if jitterRange == 0 {
		jitterRange = 1
	}
	j := time.Duration(rand.Int64N(int64(jitterRange * 2)))
	return delay - jitterRange + j
}

func normalizeRetry(r *RetryConfig) {
	if r.MaxAttempts <= 0 {
		*r = DefaultRetryConfig()
	}
	if r.InitialDelay <= 0 {
		r.InitialDelay = DefaultRetryConfig().InitialDelay
	}
	if r.MaxDelay <= 0 {
		r.MaxDelay = DefaultRetryConfig().MaxDelay
	}
	if r.Multiplier <= 0 {
		r.Multiplier = DefaultRetryConfig().Multiplier
	}
	if r.MaxDelay < r.InitialDelay {
		r.MaxDelay = r.InitialDelay
	}
}
