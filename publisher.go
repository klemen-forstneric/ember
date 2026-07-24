package ember

import "context"

// IDer
type IDer interface {
	ID() string
}

// Publisher
type Publisher struct {
	builder    envelopeBuilder
	repository EventRepository
	notifier   Notifier
}

func NewPublisher(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler, n Notifier) *Publisher {
	return &Publisher{
		builder:    envelopeBuilder{ider: i, metadata: mg, marshaler: m},
		repository: r,
		notifier:   n,
	}
}

func (p *Publisher) Publish(ctx context.Context, events ...Event) error {
	envelopes, err := p.builder.build(ctx, events...)
	if err != nil {
		return err
	}
	if err := p.repository.Save(ctx, envelopes); err != nil {
		return err
	}
	p.notifier.Notify(ctx, envelopes)
	return nil
}
