package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/mock"
)

// mockProducer is a testify mock for the producer interface. SendAsync invokes
// the callback synchronously (as the SDK does) with the configured send error
// (Return arg 0), so the Publisher observes success or failure through it.
type mockProducer struct {
	mock.Mock
}

func (m *mockProducer) SendAsync(ctx context.Context, msg *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	args := m.Called(ctx, msg, cb)
	cb(nil, msg, args.Error(0))
}

func (m *mockProducer) Close() {
	m.Called()
}

// sent flattens every ProducerMessage passed to SendAsync, in call order.
func (m *mockProducer) sent() []*pulsar.ProducerMessage {
	var out []*pulsar.ProducerMessage
	for _, c := range m.Calls {
		if c.Method == "SendAsync" {
			out = append(out, c.Arguments.Get(1).(*pulsar.ProducerMessage))
		}
	}
	return out
}

// mockProducerRegistry is a testify mock for the producerRegistry interface.
type mockProducerRegistry struct {
	mock.Mock
}

func (m *mockProducerRegistry) Get(ctx context.Context, eventType string) (producer, error) {
	args := m.Called(ctx, eventType)
	var p producer
	if v := args.Get(0); v != nil {
		p = v.(producer)
	}
	return p, args.Error(1)
}

func (m *mockProducerRegistry) Close() error {
	return m.Called().Error(0)
}

// mockConsumer is a testify mock for the consumer interface. Chan is
// channel-backed (the standard testify pattern for a streaming method): tests
// push onto in to drive delivery. Ack/Nack/MaxDeliveries go through mock.Mock;
// callCount reports how many times a method was invoked.
type mockConsumer struct {
	mock.Mock
	in chan pulsar.ConsumerMessage
}

func newMockConsumer() *mockConsumer {
	return &mockConsumer{in: make(chan pulsar.ConsumerMessage, 8)}
}

func (m *mockConsumer) Chan() <-chan pulsar.ConsumerMessage {
	return m.in
}

func (m *mockConsumer) Ack(msg pulsar.Message) error {
	return m.Called(msg).Error(0)
}

func (m *mockConsumer) Nack(msg pulsar.Message) {
	m.Called(msg)
}

func (m *mockConsumer) Close() {
	m.Called()
}

func (m *mockConsumer) MaxDeliveries() (int, bool) {
	args := m.Called()
	return args.Int(0), args.Bool(1)
}

// callCount reports how many times method was invoked. Safe to read once the
// subscriber goroutines have stopped (after Stop).
func (m *mockConsumer) callCount(method string) int {
	n := 0
	for _, c := range m.Calls {
		if c.Method == method {
			n++
		}
	}
	return n
}

// mockConsumerRegistry is a testify mock for the consumerRegistry interface.
type mockConsumerRegistry struct {
	mock.Mock
}

func (m *mockConsumerRegistry) Get(ctx context.Context, subscription string) ([]consumer, error) {
	args := m.Called(ctx, subscription)
	var cs []consumer
	if v := args.Get(0); v != nil {
		cs = v.([]consumer)
	}
	return cs, args.Error(1)
}

func (m *mockConsumerRegistry) Close() error {
	return m.Called().Error(0)
}
