package pulsar

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// producer is the narrow slice of *pulsar.Producer the Publisher needs.
// Close() is void to match the SDK, so *pulsar.Producer satisfies this directly.
type producer interface {
	SendAsync(context.Context, *pulsar.ProducerMessage, func(pulsar.MessageID, *pulsar.ProducerMessage, error))
	Close()
}

// EntityPlaceholder in a route stands for the event's entity id, giving each
// entity its own topic, e.g. "non-persistent://public/default/chat.ws.{entity}".
const EntityPlaceholder = "{entity}"

// ProducerRegistry routes event types to topics and lazily creates +
// caches one *pulsar.Producer per topic. Producers for different topics are
// created concurrently. With per-entity routes the topics keep coming, so run
// Run to close producers once they go idle.
type ProducerRegistry struct {
	client pulsar.Client
	routes map[string]string // eventType -> topic

	mu        sync.Mutex
	producers map[string]*producerEntry // topic -> producer
}

// producerEntry is one topic's producer; ready closes once creation finished,
// after which p and err are fixed.
type producerEntry struct {
	ready    chan struct{}
	p        producer
	err      error
	lastUsed time.Time // guarded by ProducerRegistry.mu
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
func (r *ProducerRegistry) Get(_ context.Context, eventType, entityID string) (producer, error) {
	topic, ok := r.routes[eventType]
	if !ok {
		return nil, fmt.Errorf("no topic configured for event type %q", eventType)
	}
	if strings.Contains(topic, EntityPlaceholder) {
		if entityID == "" {
			return nil, fmt.Errorf("event type %q routes per entity but has no entity id", eventType)
		}
		topic = strings.ReplaceAll(topic, EntityPlaceholder, entityID)
	}

	r.mu.Lock()
	e, ok := r.producers[topic]
	if ok {
		e.lastUsed = time.Now()
		r.mu.Unlock()
		<-e.ready
		return e.p, e.err
	}
	e = &producerEntry{ready: make(chan struct{}), lastUsed: time.Now()}
	r.producers[topic] = e
	r.mu.Unlock()

	// Created outside the lock: a slow topic must not hold up the other topics' sends.
	e.p, e.err = r.client.CreateProducer(pulsar.ProducerOptions{
		Topic:              topic,
		BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
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

// Run closes producers unused for idle, until ctx ends.
func (r *ProducerRegistry) Run(ctx context.Context, idle time.Duration) {
	t := time.NewTicker(idle / 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			r.closeIdle(now, idle)
		}
	}
}

func (r *ProducerRegistry) closeIdle(now time.Time, idle time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for topic, e := range r.producers {
		select {
		case <-e.ready:
		default:
			continue // still being created
		}
		if e.err == nil && now.Sub(e.lastUsed) >= idle {
			e.p.Close()
			delete(r.producers, topic)
		}
	}
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
