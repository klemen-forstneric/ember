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
				case cmsg := <-c.Chan():
					var m message
					if err := json.Unmarshal(cmsg.Payload(), &m); err != nil {
						s.logger.Error(ctx, "Could not unmarshal the message", err)
						continue
					}

					metadata := m.Metadata
					if metadata == nil {
						metadata = make(ember.Metadata)
					}
					metadata[MetadataKeyCurrentDelivery] = int(cmsg.RedeliveryCount())
					metadata[MetadataKeyMaxDeliveries] = c.MaxDeliveries()
					metadata[MetadataKeyCorrelationID] = m.CorrelationID

					msg := cmsg.Message
					envelope := ember.AckableEventEnvelope{
						EventEnvelope: ember.EventEnvelope{
							ID:        m.ID,
							EntityID:  m.EntityID,
							Event:     &ember.MarshaledEvent{Type: m.Type, Data: m.Data},
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

// Stop signals all forwarding goroutines to exit, waits for them to drain, then
// releases the consumers via the registry. The out channels are intentionally
// not closed: the downstream ember.Consumer terminates via its own Stop()/ctx.
func (s *Subscriber) Stop() {
	close(s.shutdown)
	s.wg.Wait()
	if err := s.registry.Close(); err != nil {
		s.logger.Error(context.Background(), "Could not close consumer registry", err)
	}
}
