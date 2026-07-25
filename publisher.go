package ember

import (
	"context"
	"errors"
)

// ErrDeliveryFailed marks a delivery that failed after its transaction
// committed. Reachable only under BestEffort.
var ErrDeliveryFailed = errors.New("ember: event delivery failed after commit")

// IDer
type IDer interface {
	ID() string
}

// delivery is work that must run only after the transaction commits.
type delivery func(ctx context.Context) error

// Guarantee is the delivery guarantee a Publisher was built with. Sealed: only
// AtLeastOnce and BestEffort implement it.
type Guarantee interface {
	// stage performs the guarantee's durable work inside the caller's
	// transaction and returns any delivery deferred until after commit.
	stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error)
}

type atLeastOnce struct {
	repo EventRepository
}

func (g atLeastOnce) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	return nil, g.repo.Save(ctx, envelopes)
}

type bestEffort struct {
	sink Sink
}

func (g bestEffort) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	return func(ctx context.Context) error {
		return g.sink.Publish(ctx, envelopes)
	}, nil
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

// AtLeastOnce persists envelopes to the outbox inside the caller's transaction.
// A Relay draining that outbox is the sole publisher.
func AtLeastOnce(r EventRepository) Guarantee {
	return atLeastOnce{repo: r}
}

// BestEffort persists nothing and pushes to the Sink after EntitySaver commits.
// Pass every entity to a single Save call — wrapping Save in your own
// transaction defers the commit past delivery, so a rollback leaves a
// published event for state that never existed. A crash between commit and
// push loses the event.
func BestEffort(s Sink) Guarantee {
	return bestEffort{sink: s}
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
