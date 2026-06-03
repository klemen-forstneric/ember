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
// caches one *pulsar.Producer per topic.
type ProducerRegistry struct {
	client pulsar.Client
	routes map[string]string // eventType -> topic

	mu        sync.Mutex
	producers map[string]producer // topic -> producer
}

func NewProducerRegistry(client pulsar.Client, routes map[string]string) *ProducerRegistry {
	return &ProducerRegistry{
		client:    client,
		routes:    routes,
		producers: map[string]producer{},
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
	defer r.mu.Unlock()
	if p, ok := r.producers[topic]; ok {
		return p, nil
	}
	p, err := r.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		return nil, fmt.Errorf("could not create producer for topic %q: %w", topic, err)
	}
	r.producers[topic] = p
	return p, nil
}

func (r *ProducerRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.producers {
		p.Close()
	}
	return nil
}
