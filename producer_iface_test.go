package rabbitmq

import "testing"

func TestProducerInterface(t *testing.T) {
	var _ Producer = (*fakeProducer)(nil)
}
