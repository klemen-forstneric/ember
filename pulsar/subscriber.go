package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/klemen-forstneric/ember"
)

// Subscriber is the Pulsar implementation of ember.Source. It resolves a
// subscription name to one or more consumers via the registry and fans their
// messages into a single channel.
var (
	_ ember.Source       = (*Subscriber)(nil)
	_ ember.Unsubscriber = (*Subscriber)(nil)
)

// consumerRegistry resolves the consumers for a subscription name (fan-in:
// one subscription may map to several consumers). Get returns an error for an
// unknown subscription.
type consumerRegistry interface {
	Get(ctx context.Context, subscription string) ([]consumer, error)
	Release(subscription string)
	Close() error
}

type Subscriber struct {
	registry consumerRegistry
	logger   ember.LoggerCtx

	shutdown chan struct{}
	wg       sync.WaitGroup

	mu   sync.Mutex
	subs map[string]*subscription
}

// subscription is one name's pumps; closing done stops them and closes its channel.
type subscription struct {
	done chan struct{}
	wg   sync.WaitGroup
}

func NewSubscriber(r consumerRegistry, l ember.LoggerCtx) *Subscriber {
	return &Subscriber{
		registry: r,
		logger:   l,
		shutdown: make(chan struct{}),
		subs:     map[string]*subscription{},
	}
}

func (s *Subscriber) Subscribe(ctx context.Context, name string) (<-chan ember.AckableEventEnvelope, error) {
	s.mu.Lock()
	if _, ok := s.subs[name]; ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("already subscribed to %q", name)
	}
	sub := &subscription{done: make(chan struct{})}
	s.subs[name] = sub
	s.mu.Unlock()

	consumers, err := s.registry.Get(ctx, name)
	if err != nil {
		s.mu.Lock()
		delete(s.subs, name)
		s.mu.Unlock()
		return nil, err
	}

	out := make(chan ember.AckableEventEnvelope)

	for _, c := range consumers {
		s.wg.Add(1)
		sub.wg.Add(1)
		go func(c consumer) {
			defer s.wg.Done()
			defer sub.wg.Done()

			for {
				select {
				case msg := <-c.Chan():
					var m message
					if err := json.Unmarshal(msg.Payload(), &m); err != nil {
						s.logger.Error(ctx, "Could not unmarshal the message", err)
						continue
					}

					metadata := m.Metadata
					if metadata == nil {
						metadata = make(ember.Metadata)
					}

					metadata[MetadataKeyCorrelationID] = m.CorrelationID
					metadata[ember.MetadataKeyCurrentDelivery] = int(msg.RedeliveryCount() + 1)
					if v, ok := c.MaxDeliveries(); ok {
						metadata[ember.MetadataKeyMaxDeliveries] = v
					}

					envelope := ember.AckableEventEnvelope{
						EventEnvelope: ember.EventEnvelope{
							ID:          m.ID,
							EntityID:    m.EntityID,
							OrderingKey: msg.Key(),
							Event: &ember.MarshaledEvent{
								Type: m.Type,
								Data: m.Data,
							},
							Metadata:  metadata,
							Timestamp: m.PublishedAt,
						},
						Ack: func() {
							if err := c.Ack(msg); err != nil {
								s.logger.Error(ctx, "Could not acknowledge the event", err)
							}
						},
						Nack: func() { c.Nack(msg) },
					}

					select {
					case out <- envelope:
					case <-s.shutdown:
						return
					case <-sub.done:
						return
					}
				case <-s.shutdown:
					return
				case <-sub.done:
					return
				}
			}
		}(c)
	}

	// Closing the channel tells the consumer reading it that the subscription ended.
	go func() {
		sub.wg.Wait()
		close(out)
	}()

	return out, nil
}

// Unsubscribe stops one subscription's delivery, closes its channel and releases its consumers.
func (s *Subscriber) Unsubscribe(name string) error {
	s.mu.Lock()
	sub, ok := s.subs[name]
	delete(s.subs, name)
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("not subscribed to %q", name)
	}

	close(sub.done)
	sub.wg.Wait() // the pumps stop reading before their consumers close
	s.registry.Release(name)
	return nil
}

func (s *Subscriber) Stop() {
	ctx := context.Background()

	close(s.shutdown)
	s.wg.Wait()
	if err := s.registry.Close(); err != nil {
		s.logger.Error(ctx, "Could not close consumer registry", err)
	}
}
