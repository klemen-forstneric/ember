package noop

import (
	"context"

	"github.com/klemen-forstneric/ember"
)

type Notifier struct {
}

func (n *Notifier) Notify(ctx context.Context, envelopes []ember.EventEnvelope) {
}
