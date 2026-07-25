package pulsar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/klemen-forstneric/ember"
)

// defaultMaxMessageSize matches Pulsar's default maxMessageSize.
const defaultMaxMessageSize = 5 * 1024 * 1024

// producerRegistry resolves the producer for an event type, creating and
// caching it on demand. Get returns an error for an unmapped event type.
type producerRegistry interface {
	Get(ctx context.Context, eventType string) (producer, error)
	Close() error
}

// Publisher sends marshaled event envelopes onto Pulsar topics, routing each
// event to its topic via the producerRegistry.
type Publisher struct {
	registry       producerRegistry
	maxMessageSize int
}

// NewPublisher builds a Publisher. maxMessageSize caps the marshaled payload
// size; 0 uses defaultMaxMessageSize.
func NewPublisher(r producerRegistry, maxMessageSize int) *Publisher {
	if maxMessageSize <= 0 {
		maxMessageSize = defaultMaxMessageSize
	}
	return &Publisher{registry: r, maxMessageSize: maxMessageSize}
}

func (p *Publisher) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	type pending struct {
		prod producer
		msg  *pulsar.ProducerMessage
	}

	prepared := make([]pending, 0, len(envelopes))

	for _, e := range envelopes {
		correlationID, ok := e.Metadata[MetadataKeyCorrelationID].(string)
		if !ok {
			return fmt.Errorf("invalid metadata, missing key '%v'", MetadataKeyCorrelationID)
		}

		prod, err := p.registry.Get(ctx, e.Event.Type)
		if err != nil {
			return err
		}

		payload, err := json.Marshal(&message{
			ID:            e.ID,
			CorrelationID: correlationID,
			EntityID:      e.EntityID,
			Type:          e.Event.Type,
			Data:          e.Event.Data,
			Metadata:      e.Metadata,
			PublishedAt:   e.Timestamp,
		})
		if err != nil {
			return err
		}

		if len(payload) > p.maxMessageSize {
			return fmt.Errorf("envelope %q: marshaled payload %d bytes exceeds max %d", e.ID, len(payload), p.maxMessageSize)
		}

		prepared = append(prepared, pending{
			prod: prod,
			msg: &pulsar.ProducerMessage{
				Key:       e.EntityID,
				Payload:   payload,
				EventTime: e.Timestamp,
			},
		})
	}

	errs := make([]error, len(prepared))

	var wg sync.WaitGroup
	for i, pm := range prepared {
		wg.Add(1)
		i, pm := i, pm
		pm.prod.SendAsync(ctx, pm.msg, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
			errs[i] = err
			wg.Done()
		})
	}
	wg.Wait()

	return errors.Join(errs...)
}

func (p *Publisher) Close() error {
	return p.registry.Close()
}

var _ ember.Sink = (*Publisher)(nil)
