package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

// producer is the narrow slice of *pulsar.Producer the Publisher needs.
// Close() is void to match the SDK, so *pulsar.Producer satisfies this directly.
type producer interface {
	SendAsync(context.Context, *pulsar.ProducerMessage, func(pulsar.MessageID, *pulsar.ProducerMessage, error))
	Close()
}

// consumer is the slice of *pulsar.Consumer the Subscriber needs, plus
// MaxDeliveries (the per-consumer retry budget the Subscriber stamps onto
// events). MaxDeliveries means the registry, not the Subscriber, interprets the
// Pulsar DLQ config; the registry wraps the raw SDK consumer to supply it.
type consumer interface {
	Chan() <-chan pulsar.ConsumerMessage
	Ack(pulsar.Message) error
	Nack(pulsar.Message)
	Close()
	MaxDeliveries() int
}

// producerRegistry resolves the producer for an event type, creating and
// caching it on demand. Get returns an error for an unmapped event type.
type producerRegistry interface {
	Get(ctx context.Context, eventType string) (producer, error)
	Close() error
}

// consumerRegistry resolves the consumers for a subscription name (fan-in:
// one subscription may map to several consumers). Get returns an error for an
// unknown subscription.
type consumerRegistry interface {
	Get(ctx context.Context, subscription string) ([]consumer, error)
	Close() error
}
