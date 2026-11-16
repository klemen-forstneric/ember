package ember

import (
	"context"
	"errors"
	"fmt"
)

// IDer
type IDer interface {
	ID() string
}

// Publisher builds event envelopes and hands each to the guarantee its options
// give its event.
type Publisher struct {
	builder    envelopeBuilder
	guarantees []guarantee
	byType     map[string]int // event type to its guarantee
	fallback   int            // the guarantee of every unlisted event; -1 when there is none
}

// NewPublisher delivers each event with the guarantee of the option that lists
// it, and every unlisted event with the one option that lists none. It panics
// on a wiring mistake: no option, an event type listed twice, or two options
// that list no events.
func NewPublisher(i IDer, mg MetadataGetter, m EventMarshaler, opts ...PublisherOption) *Publisher {
	if len(opts) == 0 {
		panic("ember: a Publisher needs a delivery guarantee")
	}
	p := &Publisher{
		builder:  envelopeBuilder{ider: i, metadata: mg, marshaler: m},
		byType:   map[string]int{},
		fallback: -1,
	}
	for idx, o := range opts {
		p.guarantees = append(p.guarantees, o.guarantee)
		if len(o.events) == 0 {
			if p.fallback >= 0 {
				panic("ember: two delivery guarantees cover every unlisted event")
			}
			p.fallback = idx
			continue
		}
		for _, e := range o.events {
			if _, listed := p.byType[e.Type()]; listed {
				panic(fmt.Sprintf("ember: event %s has two delivery guarantees", e.Type()))
			}
			p.byType[e.Type()] = idx
		}
	}
	return p
}

// stage stages each guarantee's events, in emit order, and returns one delivery
// for every guarantee that defers one.
func (p *Publisher) stage(ctx context.Context, events []staged) (delivery, error) {
	if len(events) == 0 {
		return nil, nil
	}

	envelopes, err := p.builder.build(ctx, events)
	if err != nil {
		return nil, err
	}
	groups := make([][]EventEnvelope, len(p.guarantees))
	for i, e := range events {
		idx, ok := p.byType[e.event.Type()]
		if !ok {
			if p.fallback < 0 {
				return nil, fmt.Errorf("%w: %s", ErrNoGuarantee, e.event.Type())
			}
			idx = p.fallback
		}
		groups[idx] = append(groups[idx], envelopes[i])
	}

	var deliveries []delivery
	for idx, envs := range groups {
		if len(envs) == 0 {
			continue
		}
		d, err := p.guarantees[idx].stage(ctx, envs)
		if err != nil {
			return nil, err
		}
		if d != nil {
			deliveries = append(deliveries, d)
		}
	}

	switch len(deliveries) {
	case 0:
		return nil, nil
	case 1:
		return deliveries[0], nil
	}
	return func(ctx context.Context) error {
		var errs []error
		for _, d := range deliveries {
			errs = append(errs, d(ctx))
		}
		return errors.Join(errs...)
	}, nil
}

// Publish is the entity-less path: no transaction is in scope, so a deferred
// delivery runs immediately.
func (p *Publisher) Publish(ctx context.Context, events ...Event) error {
	st := make([]staged, 0, len(events))
	for i, e := range events {
		st = append(st, staged{event: e, index: i})
	}

	d, err := p.stage(ctx, st)
	if err != nil {
		return err
	}
	if d == nil {
		return nil
	}
	return d(ctx)
}
