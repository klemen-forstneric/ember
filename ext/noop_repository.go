package ext

import (
	"context"

	"github.com/klemen-forstneric/ember"
)

type NoopRepository struct {
}

func (r *NoopRepository) Save(ctx context.Context, envelopes []ember.EventEnvelope) error {
	return nil
}
