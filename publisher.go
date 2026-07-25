package ember

import "context"

// IDer
type IDer interface {
	ID() string
}

// Publisher builds event envelopes and hands them to its guarantee.
type Publisher struct {
	builder   envelopeBuilder
	guarantee Guarantee
}

func NewPublisher(i IDer, mg MetadataGetter, m EventMarshaler, g Guarantee) *Publisher {
	return &Publisher{
		builder:   envelopeBuilder{ider: i, metadata: mg, marshaler: m},
		guarantee: g,
	}
}

func (p *Publisher) stage(ctx context.Context, events ...Event) (delivery, error) {
	if len(events) == 0 {
		return nil, nil
	}

	envelopes, err := p.builder.build(ctx, events...)
	if err != nil {
		return nil, err
	}
	return p.guarantee.stage(ctx, envelopes)
}

// Publish is the entity-less path: no transaction is in scope, so a deferred
// delivery runs immediately.
func (p *Publisher) Publish(ctx context.Context, events ...Event) error {
	d, err := p.stage(ctx, events...)
	if err != nil || d == nil {
		return err
	}
	return d(ctx)
}
