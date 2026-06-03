package ember

import "context"

// Notifier
type Notifier interface {
	Notify(ctx context.Context, envelopes []EventEnvelope)
}
