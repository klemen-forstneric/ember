package ember

import (
	"context"
	"errors"
)

// ErrDeliveryFailed marks a delivery that failed after its transaction
// committed. Reachable only under WithBestEffort.
var ErrDeliveryFailed = errors.New("ember: event delivery failed after commit")

// ErrNoGuarantee marks an event that no option of its Publisher covers.
var ErrNoGuarantee = errors.New("ember: no delivery guarantee for event")

// delivery is work that must run only after the transaction commits.
type delivery func(ctx context.Context) error

// guarantee is how a Publisher delivers some of its events. Sealed: only
// WithAtLeastOnce and WithBestEffort make one.
type guarantee interface {
	// stage performs the guarantee's durable work inside the caller's
	// transaction and returns any delivery deferred until after commit.
	stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error)
}

// PublisherOption gives a Publisher a delivery guarantee for the events it
// lists, or, when it lists none, for every event no other option lists.
type PublisherOption struct {
	guarantee guarantee
	events    []Event
}

type atLeastOnce struct {
	repo EventRepository
}

// WithAtLeastOnce persists the events' envelopes to the outbox inside the
// caller's transaction. A relay is the sole publisher: PollingRelay for a
// table-backed outbox, or postgres/wal.Relay for a WAL-backed one.
func WithAtLeastOnce(r EventRepository, events ...Event) PublisherOption {
	return PublisherOption{
		guarantee: atLeastOnce{repo: r},
		events:    events,
	}
}

func (g atLeastOnce) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	return nil, g.repo.Save(ctx, envelopes)
}

type bestEffort struct {
	sink Sink
}

// WithBestEffort persists nothing and pushes the events to the Sink after
// EntitySaver commits. Pass every entity to a single Save call — wrapping Save
// in your own transaction defers the commit past delivery, so a rollback leaves
// a published event for state that never existed. A crash between commit and
// push loses the event.
func WithBestEffort(s Sink, events ...Event) PublisherOption {
	return PublisherOption{
		guarantee: bestEffort{sink: s},
		events:    events,
	}
}

func (g bestEffort) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	return func(ctx context.Context) error {
		return g.sink.Publish(ctx, envelopes)
	}, nil
}
