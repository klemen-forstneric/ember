package ember

import "context"

// Sink delivers marshaled event envelopes to the broker.
type Sink interface {
	Publish(ctx context.Context, envelopes []EventEnvelope) error
}
