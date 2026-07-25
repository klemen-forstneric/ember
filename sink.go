package ember

import "context"

// Sink delivers marshaled event envelopes to the broker.
type Sink interface {
	// Publish sends envelopes in order and returns the length of the leading run
	// that was published. A returned n means envelopes[:n] are delivered and
	// envelopes[n:] are not, so the caller may mark exactly that prefix.
	Publish(ctx context.Context, envelopes []EventEnvelope) (int, error)
}
