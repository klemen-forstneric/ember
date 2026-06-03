package noop

import (
	"context"

	"github.com/klemen-forstneric/ember"
)

type EventRepository struct {
}

func (r *EventRepository) Save(ctx context.Context, envelopes []ember.EventEnvelope) error {
	return nil
}
