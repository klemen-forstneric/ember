package pulsar

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

// clientProducerRegistry routes event types to topics and lazily creates +
// caches one *pulsar.Producer per topic.
type clientProducerRegistry struct {
	client pulsar.Client
	routes map[string]string // eventType -> topic

	mu        sync.Mutex
	producers map[string]producer // topic -> producer
}

func NewProducerRegistry(client pulsar.Client, routes map[string]string) *clientProducerRegistry {
	return &clientProducerRegistry{
		client:    client,
		routes:    routes,
		producers: map[string]producer{},
	}
}

func (r *clientProducerRegistry) Get(_ context.Context, eventType string) (producer, error) {
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

func (r *clientProducerRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.producers {
		p.Close()
	}
	return nil
}

// clientConsumerRegistry maps a subscription name to one or more consumers,
// created eagerly from the configured pulsar.ConsumerOptions on Get.
type clientConsumerRegistry struct {
	client pulsar.Client
	config map[string][]pulsar.ConsumerOptions

	mu      sync.Mutex
	created []consumer
}

func NewConsumerRegistry(client pulsar.Client, config map[string][]pulsar.ConsumerOptions) *clientConsumerRegistry {
	return &clientConsumerRegistry{client: client, config: config}
}

func (r *clientConsumerRegistry) Get(_ context.Context, subscription string) ([]subscriptionConsumer, error) {
	opts, ok := r.config[subscription]
	if !ok {
		return nil, fmt.Errorf("no consumer options configured for subscription %q", subscription)
	}

	scs := make([]subscriptionConsumer, 0, len(opts))
	for _, opt := range opts {
		c, err := r.client.Subscribe(opt)
		if err != nil {
			return nil, fmt.Errorf("could not create consumer for subscription %q: %w", subscription, err)
		}
		r.mu.Lock()
		r.created = append(r.created, c)
		r.mu.Unlock()

		scs = append(scs, subscriptionConsumer{
			consumer:      c,
			maxDeliveries: maxDeliveries(opt),
		})
	}
	return scs, nil
}

func (r *clientConsumerRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.created {
		c.Close()
	}
	return nil
}

// maxDeliveries derives the per-consumer max delivery count, guarding the nil
// DLQ case. It mirrors the broker's "deliveries before DLQ" minus the first
// delivery, matching how the Subscriber stamps current vs. max.
func maxDeliveries(opt pulsar.ConsumerOptions) int {
	if opt.DLQ == nil {
		return 0
	}
	return int(opt.DLQ.MaxDeliveries) - 1
}
