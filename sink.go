package ember

import "context"

// Sink delivers marshaled event envelopes to the broker.
type Sink interface {
	// Publish sends envelopes in order. It is all-or-nothing from the caller's
	// perspective: on error the caller must treat every envelope as unpublished.
	Publish(ctx context.Context, envelopes []EventEnvelope) error
}
