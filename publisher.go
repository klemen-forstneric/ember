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

// Publisher builds event envelopes and hands each to the guarantee of the
// option that covers its event.
type Publisher struct {
	builder  envelopeBuilder
	listed   map[string]*PublisherOption // each listed event type to the option that lists it
	unlisted *PublisherOption            // covers every event no option lists; nil when there is none
}

// NewPublisher delivers each event with the guarantee of the option that lists
// it, and every unlisted event with the one option that lists none. It panics
// on a wiring mistake: no option, an event type listed twice, or two options
// that list no events.
func NewPublisher(i IDer, mg MetadataGetter, m EventMarshaler, opts ...PublisherOption) *Publisher {
	if len(opts) == 0 {
		panic("ember: a Publisher needs a delivery guarantee")
	}
	o := options(opts)
	return &Publisher{
		builder:  envelopeBuilder{ider: i, metadata: mg, marshaler: m},
		listed:   o.listed(),
		unlisted: o.unlisted(),
	}
}

// stage stages each option's events with its guarantee, in emit order, and
// returns one delivery for every guarantee that defers one.
func (p *Publisher) stage(ctx context.Context, events []staged) (delivery, error) {
	if len(events) == 0 {
		return nil, nil
	}

	envelopes, err := p.builder.build(ctx, events)
	if err != nil {
		return nil, err
	}
	var order []*PublisherOption
	groups := map[*PublisherOption][]EventEnvelope{}
	for i, e := range events {
		o, err := p.option(e.event)
		if err != nil {
			return nil, err
		}
		if _, seen := groups[o]; !seen {
			order = append(order, o)
		}
		groups[o] = append(groups[o], envelopes[i])
	}

	var deliveries []delivery
	for _, o := range order {
		d, err := o.guarantee.stage(ctx, groups[o])
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

// option is the option that covers e: the one that lists it, else the unlisted one.
func (p *Publisher) option(e Event) (*PublisherOption, error) {
	if o, ok := p.listed[e.Type()]; ok {
		return o, nil
	}
	if p.unlisted == nil {
		return nil, fmt.Errorf("%w: %s", ErrNoGuarantee, e.Type())
	}
	return p.unlisted, nil
}

// options
type options []PublisherOption

// listed maps each event type an option lists to that option; it panics on a
// type listed twice.
func (o options) listed() map[string]*PublisherOption {
	out := map[string]*PublisherOption{}
	for i := range o {
		for _, e := range o[i].events {
			if _, twice := out[e.Type()]; twice {
				panic(fmt.Sprintf("ember: event %s has two delivery guarantees", e.Type()))
			}
			out[e.Type()] = &o[i]
		}
	}
	return out
}

// unlisted is the option that lists no events; nil when there is none. It
// panics on two.
func (o options) unlisted() *PublisherOption {
	var out *PublisherOption
	for i := range o {
		if len(o[i].events) > 0 {
			continue
		}
		if out != nil {
			panic("ember: two delivery guarantees cover every unlisted event")
		}
		out = &o[i]
	}
	return out
}
