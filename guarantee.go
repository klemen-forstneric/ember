package ember

import (
	"context"
	"errors"
)

// ErrDeliveryFailed marks a delivery that failed after its transaction
// committed. Reachable only under BestEffort.
var ErrDeliveryFailed = errors.New("ember: event delivery failed after commit")

// delivery is work that must run only after the transaction commits.
type delivery func(ctx context.Context) error

// Guarantee is the delivery guarantee a Publisher was built with. Sealed: only
// AtLeastOnce, BestEffort and Split implement it.
type Guarantee interface {
	// stage performs the guarantee's durable work inside the caller's
	// transaction and returns any delivery deferred until after commit.
	stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error)
}

type atLeastOnce struct {
	repo EventRepository
}

// AtLeastOnce persists envelopes to the outbox inside the caller's transaction.
// A relay is the sole publisher: PollingRelay for a table-backed outbox, or
// postgres/wal.Relay for a WAL-backed one.
func AtLeastOnce(r EventRepository) Guarantee {
	return atLeastOnce{repo: r}
}

func (g atLeastOnce) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	return nil, g.repo.Save(ctx, envelopes)
}

type bestEffort struct {
	sink Sink
}

// BestEffort persists nothing and pushes to the Sink after EntitySaver commits.
// Pass every entity to a single Save call — wrapping Save in your own
// transaction defers the commit past delivery, so a rollback leaves a
// published event for state that never existed. A crash between commit and
// push loses the event.
func BestEffort(s Sink) Guarantee {
	return bestEffort{sink: s}
}

func (g bestEffort) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	return func(ctx context.Context) error {
		return g.sink.Publish(ctx, envelopes)
	}, nil
}

type split struct {
	rest  Guarantee
	by    Guarantee
	types map[string]bool
}

// Split stages events like the given ones with by and every other event with
// rest, so one Publisher, and so one EntitySaver, can save entities whose events
// need different guarantees in one transaction: Split(AtLeastOnce(outbox),
// BestEffort(sink), &MessageAdded{}) keeps MessageAdded off the outbox and
// delivers it only after commit. Events are matched by type, as the marshaler
// registers them. Each side keeps emit order.
func Split(rest, by Guarantee, events ...Event) Guarantee {
	g := split{rest: rest, by: by, types: map[string]bool{}}
	for _, e := range events {
		g.types[e.Type()] = true
	}
	return g
}

func (g split) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	var rest, by []EventEnvelope
	for _, e := range envelopes {
		if g.types[e.Event.Type] {
			by = append(by, e)
		} else {
			rest = append(rest, e)
		}
	}

	var deliveries []delivery
	for _, side := range []struct {
		guarantee Guarantee
		envelopes []EventEnvelope
	}{{g.rest, rest}, {g.by, by}} {
		if len(side.envelopes) == 0 {
			continue
		}
		d, err := side.guarantee.stage(ctx, side.envelopes)
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
		return errors.Join(deliveries[0](ctx), deliveries[1](ctx))
	}, nil
}
