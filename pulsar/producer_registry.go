package pulsar

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

// producer is the narrow slice of *pulsar.Producer the Publisher needs.
// Close() is void to match the SDK, so *pulsar.Producer satisfies this directly.
type producer interface {
	SendAsync(context.Context, *pulsar.ProducerMessage, func(pulsar.MessageID, *pulsar.ProducerMessage, error))
	Close()
}

// ProducerRegistry routes event types to topics and lazily creates +
// caches one *pulsar.Producer per topic. Producers for different topics are
// created concurrently.
type ProducerRegistry struct {
	client pulsar.Client
	routes map[string]string // eventType -> topic

	mu        sync.Mutex
	producers map[string]*producerEntry // topic -> producer
}

// producerEntry is one topic's producer; ready closes once creation finished,
// after which p and err are fixed.
type producerEntry struct {
	ready chan struct{}
	p     producer
	err   error
}

func NewProducerRegistry(client pulsar.Client, routes map[string]string) *ProducerRegistry {
	return &ProducerRegistry{
		client:    client,
		routes:    routes,
		producers: map[string]*producerEntry{},
	}
}

// Get ignores ctx: the pulsar-client-go v0.19.0 CreateProducer API takes only
// options, so there is nothing to thread it into. The parameter is kept for
// interface symmetry and forward compatibility.
func (r *ProducerRegistry) Get(_ context.Context, eventType string) (producer, error) {
	topic, ok := r.routes[eventType]
	if !ok {
		return nil, fmt.Errorf("no topic configured for event type %q", eventType)
	}

	r.mu.Lock()
	e, ok := r.producers[topic]
	if ok {
		r.mu.Unlock()
		<-e.ready
		return e.p, e.err
	}
	e = &producerEntry{ready: make(chan struct{})}
	r.producers[topic] = e
	r.mu.Unlock()

	// Created outside the lock: a slow topic must not hold up the other topics' sends.
	e.p, e.err = r.client.CreateProducer(pulsar.ProducerOptions{
		Topic:              topic,
		BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
		HashingScheme:      pulsar.JavaStringHash, // Partition hashes the same way
	})
	if e.err != nil {
		e.err = fmt.Errorf("could not create producer for topic %q: %w", topic, e.err)
		r.mu.Lock()
		delete(r.producers, topic) // the next Get retries
		r.mu.Unlock()
	}
	close(e.ready)
	return e.p, e.err
}

func (r *ProducerRegistry) Close() error {
	r.mu.Lock()
	entries := r.producers
	r.producers = map[string]*producerEntry{}
	r.mu.Unlock()
	for _, e := range entries {
		<-e.ready
		if e.err == nil {
			e.p.Close()
		}
	}
	return nil
}
