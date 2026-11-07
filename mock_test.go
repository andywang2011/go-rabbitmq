package rabbitmq

import (
	"context"
	"testing"
)

type fakeProducer struct {
	bodies [][]byte
}

func (f *fakeProducer) Publish(_ context.Context, _, _ string, body []byte, _ ...PublishOption) error {
	f.bodies = append(f.bodies, body)
	return nil
}
func (f *fakeProducer) Close() error { return nil }

var _ Producer = (*fakeProducer)(nil)

type fakeConsumer struct{}

func (f *fakeConsumer) Consume(context.Context, string, MessageHandler) error { return nil }
func (f *fakeConsumer) ConsumeWithConcurrency(context.Context, string, int, MessageHandler) error {
	return nil
}
func (f *fakeConsumer) EnsureQueue(string) error { return nil }
func (f *fakeConsumer) Close() error             { return nil }

var _ Consumer = (*fakeConsumer)(nil)

func TestFakeProducer_Publish(t *testing.T) {
	f := &fakeProducer{}
	if err := f.Publish(context.Background(), "ex", "rk", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if len(f.bodies) != 1 {
		t.Fatal(f.bodies)
	}
}
