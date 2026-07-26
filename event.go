package ember

import (
	"context"
	"time"
)

// MarshaledEvent
type MarshaledEvent struct {
	Type string
	Data []byte
}

// EventMarshaler
type EventMarshaler interface {
	Marshal(ctx context.Context, e Event) (*MarshaledEvent, error)
	Unmarshal(ctx context.Context, e *MarshaledEvent) (Event, error)
}

// EventRepository is the outbox's durable write side. Save runs inside the
// caller's transaction.
type EventRepository interface {
	Save(ctx context.Context, envelopes []EventEnvelope) error
}

// EventEnvelope
type EventEnvelope struct {
	ID        string
	EntityID  string
	Event     *MarshaledEvent
	Metadata  Metadata
	Timestamp time.Time
}

// AckableEventEnvelope
type AckableEventEnvelope struct {
	EventEnvelope

	Ack  func()
	Nack func()
}

// Event
type Event interface {
	EntityID() string
	Type() string
}

// ReceivedEvent
type ReceivedEvent struct {
	Event

	ID        string
	Metadata  Metadata
	Timestamp time.Time
}

// envelopeBuilder stamps events into envelopes.
type envelopeBuilder struct {
	ider      IDer
	metadata  MetadataGetter
	marshaler EventMarshaler
}

func (b envelopeBuilder) build(ctx context.Context, events ...Event) ([]EventEnvelope, error) {
	metadata, err := b.metadata.Get(ctx)
	if err != nil {
		return nil, err
	}

	envelopes := make([]EventEnvelope, 0, len(events))
	for _, e := range events {
		marshaled, err := b.marshaler.Marshal(ctx, e)
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, EventEnvelope{
			ID:        b.ider.ID(),
			EntityID:  e.EntityID(),
			Event:     marshaled,
			Metadata:  metadata,
			Timestamp: time.Now().UTC(),
		})
	}
	return envelopes, nil
}
