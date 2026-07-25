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
// AtLeastOnce and BestEffort implement it.
type Guarantee interface {
	// stage performs the guarantee's durable work inside the caller's
	// transaction and returns any delivery deferred until after commit.
	stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error)
}

type atLeastOnce struct {
	repo EventRepository
}

// AtLeastOnce persists envelopes to the outbox inside the caller's transaction.
// A Relay draining that outbox is the sole publisher.
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
