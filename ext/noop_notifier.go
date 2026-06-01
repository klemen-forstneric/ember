package ext

import (
	"context"

	"github.com/klemen-forstneric/ember"
)

type NoopNotifier struct {
}

func (n *NoopNotifier) Notify(ctx context.Context, envelopes []ember.EventEnvelope) {
}
