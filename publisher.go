package ember

import (
	"context"
	"time"
)

// IDer
type IDer interface {
	ID() string
}

// Publisher
type Publisher struct {
	ider       IDer
	repository EventRepository
	metadata   MetadataGetter
	marshaler  EventMarshaler
	notifier   Notifier
}

func NewPublisher(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler, n Notifier) *Publisher {
	return &Publisher{
		ider:       i,
		repository: r,
		metadata:   mg,
		marshaler:  m,
		notifier:   n,
	}
}

func (p *Publisher) Publish(ctx context.Context, events ...Event) error {
	metadata, err := p.metadata.Get(ctx)
	if err != nil {
		return err
	}

	envelopes := make([]EventEnvelope, 0, len(events))
	for _, e := range events {
		marshaled, err := p.marshaler.Marshal(ctx, e)
		if err != nil {
			return err
		}

		envelopes = append(envelopes, EventEnvelope{
			ID:        p.ider.ID(),
			EntityID:  e.EntityID(),
			Event:     marshaled,
			Metadata:  metadata,
			Timestamp: time.Now().UTC(),
		})
	}

	if err := p.repository.Save(ctx, envelopes); err != nil {
		return err
	}

	p.notifier.Notify(ctx, envelopes)
	return nil
}
