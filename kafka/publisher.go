package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/klemen-forstneric/ember"
	"github.com/segmentio/kafka-go"
)

// writer is the narrow slice of *kafka.Writer the Publisher needs. A topic-less
// *kafka.Writer satisfies this directly and routes per-message by Message.Topic.
type writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Publisher sends marshaled event envelopes onto Kafka topics, routing each
// event to its topic via the routes table. A single multi-topic writer is used,
// so no per-topic producer registry is needed (unlike the pulsar package).
type Publisher struct {
	w      writer
	routes map[string]string // eventType -> topic
}

func NewPublisher(w writer, routes map[string]string) *Publisher {
	return &Publisher{w: w, routes: routes}
}

func (p *Publisher) Publish(ctx context.Context, envelopes ...ember.EventEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, 0, len(envelopes))
	for _, e := range envelopes {
		correlationID, ok := e.Metadata[MetadataKeyCorrelationID].(string)
		if !ok {
			return fmt.Errorf("invalid metadata, missing key '%v'", MetadataKeyCorrelationID)
		}

		topic, ok := p.routes[e.Event.Type]
		if !ok {
			return fmt.Errorf("no topic configured for event type %q", e.Event.Type)
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

		msgs = append(msgs, kafka.Message{
			Topic: topic,
			Key:   []byte(e.EntityID),
			Value: payload,
			Time:  e.Timestamp,
		})
	}

	return p.w.WriteMessages(ctx, msgs...)
}

func (p *Publisher) Close() error {
	return p.w.Close()
}
