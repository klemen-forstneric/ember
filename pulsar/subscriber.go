package pulsar

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/klemen-forstneric/ember"
)

// Subscriber is the Pulsar implementation of ember.Transport. It resolves a
// subscription name to one or more consumers via the registry and fans their
// messages into a single channel.
var _ ember.Transport = (*Subscriber)(nil)

// consumerRegistry resolves the consumers for a subscription name (fan-in:
// one subscription may map to several consumers). Get returns an error for an
// unknown subscription.
type consumerRegistry interface {
	Get(ctx context.Context, subscription string) ([]consumer, error)
	Close() error
}

type Subscriber struct {
	registry consumerRegistry
	logger   ember.LoggerCtx

	shutdown chan struct{}
	wg       sync.WaitGroup
}

func NewSubscriber(r consumerRegistry, l ember.LoggerCtx) *Subscriber {
	return &Subscriber{
		registry: r,
		logger:   l,
		shutdown: make(chan struct{}),
	}
}

func (s *Subscriber) Subscribe(ctx context.Context, name string) (<-chan ember.AckableEventEnvelope, error) {
	consumers, err := s.registry.Get(ctx, name)
	if err != nil {
		return nil, err
	}

	out := make(chan ember.AckableEventEnvelope)

	for _, c := range consumers {
		s.wg.Add(1)
		go func(c consumer) {
			defer s.wg.Done()

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
					metadata[MetadataKeyCurrentDelivery] = int(msg.RedeliveryCount() + 1)
					if v, ok := c.MaxDeliveries(); ok {
						metadata[MetadataKeyMaxDeliveries] = v
					}

					envelope := ember.AckableEventEnvelope{
						EventEnvelope: ember.EventEnvelope{
							ID:       m.ID,
							EntityID: m.EntityID,
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
					}
				case <-s.shutdown:
					return
				}
			}
		}(c)
	}

	return out, nil
}

func (s *Subscriber) Stop() {
	ctx := context.Background()

	close(s.shutdown)
	s.wg.Wait()
	if err := s.registry.Close(); err != nil {
		s.logger.Error(ctx, "Could not close consumer registry", err)
	}
}
