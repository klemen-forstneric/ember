package pulsar

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

// fakeProducer records every SendAsync call and invokes the callback
// synchronously with sendErr, so tests can assert routing and error handling.
type fakeProducer struct {
	mu      sync.Mutex
	sent    []*pulsar.ProducerMessage
	sendErr error
	closed  bool
}

func (f *fakeProducer) SendAsync(_ context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	f.mu.Lock()
	f.sent = append(f.sent, m)
	f.mu.Unlock()
	cb(nil, m, f.sendErr)
}

func (f *fakeProducer) Close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

// fakeProducerRegistry maps event type -> fakeProducer. A missing entry yields
// getErr (set it to simulate an unmapped type).
type fakeProducerRegistry struct {
	mu         sync.Mutex
	producers  map[string]*fakeProducer
	getErr     error
	closeCalls int
}

func newFakeProducerRegistry() *fakeProducerRegistry {
	return &fakeProducerRegistry{
		producers: map[string]*fakeProducer{},
	}
}

func (f *fakeProducerRegistry) Get(_ context.Context, eventType string) (producer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	p, ok := f.producers[eventType]
	if !ok {
		p = &fakeProducer{}
		f.producers[eventType] = p
	}
	return p, nil
}

func (f *fakeProducerRegistry) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	for _, p := range f.producers {
		p.Close()
	}
	return nil
}

// fakeConsumer drives the Subscriber: push messages onto in, and the
// Subscriber's goroutine reads them off Chan(). Ack/Nack record the message.
type fakeConsumer struct {
	in            chan pulsar.ConsumerMessage
	maxDeliveries int
	mu            sync.Mutex
	acked         []pulsar.Message
	nacked        []pulsar.Message
	closed        bool
}

func newFakeConsumer(maxDeliveries int) *fakeConsumer {
	return &fakeConsumer{in: make(chan pulsar.ConsumerMessage, 8), maxDeliveries: maxDeliveries}
}

func (f *fakeConsumer) Chan() <-chan pulsar.ConsumerMessage { return f.in }

func (f *fakeConsumer) MaxDeliveries() int { return f.maxDeliveries }

func (f *fakeConsumer) Ack(m pulsar.Message) error {
	f.mu.Lock()
	f.acked = append(f.acked, m)
	f.mu.Unlock()
	return nil
}

func (f *fakeConsumer) Nack(m pulsar.Message) {
	f.mu.Lock()
	f.nacked = append(f.nacked, m)
	f.mu.Unlock()
}

func (f *fakeConsumer) Close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

// fakeConsumerRegistry maps subscription name -> consumers to fan in.
type fakeConsumerRegistry struct {
	mu         sync.Mutex
	subs       map[string][]consumer
	getErr     error
	closeCalls int
}

func (f *fakeConsumerRegistry) Get(_ context.Context, subscription string) ([]consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	cs, ok := f.subs[subscription]
	if !ok {
		return nil, fmt.Errorf("unknown subscription %q", subscription)
	}
	return cs, nil
}

func (f *fakeConsumerRegistry) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	return nil
}
