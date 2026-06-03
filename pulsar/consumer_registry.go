package pulsar

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

// consumer is the slice of *pulsar.Consumer the Subscriber needs, plus
// MaxDeliveries (the per-consumer retry budget the Subscriber stamps onto
// events). The registry, not the Subscriber, interprets the Pulsar DLQ config,
// wrapping the raw SDK consumer to supply it. MaxDeliveries returns ok=false
// when no DLQ is configured: there is then no delivery cap (Pulsar redelivers
// indefinitely), so there is no "last delivery" to compute against.
type consumer interface {
	Chan() <-chan pulsar.ConsumerMessage
	Ack(pulsar.Message) error
	Nack(pulsar.Message)
	Close()
	MaxDeliveries() (int, bool)
}

// pulsarConsumer adapts a raw *pulsar.Consumer to the consumer interface by
// embedding it (which promotes Chan/Ack/Nack/Close) and adding the
// per-consumer MaxDeliveries derived from its DLQ config.
type pulsarConsumer struct {
	pulsar.Consumer
	maxDeliveries int
	capped        bool
}

func (c pulsarConsumer) MaxDeliveries() (int, bool) { return c.maxDeliveries, c.capped }

// ConsumerRegistry maps a subscription name to one or more consumers,
// created eagerly from the configured pulsar.ConsumerOptions on Get.
type ConsumerRegistry struct {
	client pulsar.Client
	config map[string][]pulsar.ConsumerOptions

	mu        sync.Mutex
	consumers []consumer
}

func NewConsumerRegistry(
	client pulsar.Client,
	config map[string][]pulsar.ConsumerOptions,
) *ConsumerRegistry {
	return &ConsumerRegistry{client: client, config: config}
}

// Get ignores ctx for the same reason as clientProducerRegistry.Get: the
// v0.19.0 Subscribe API takes only options.
func (r *ConsumerRegistry) Get(_ context.Context, subscription string) ([]consumer, error) {
	opts, ok := r.config[subscription]
	if !ok {
		return nil, fmt.Errorf("no consumer options configured for subscription %q", subscription)
	}

	consumers := make([]consumer, 0, len(opts))
	for _, opt := range opts {
		c, err := r.client.Subscribe(opt)
		if err != nil {
			// Release the consumers created so far in this call before failing.
			// They are not yet tracked in r.consumers (we only record them on full
			// success below), so the caller — which never reaches Stop() after a
			// Subscribe error — would otherwise leak them at the broker.
			for _, created := range consumers {
				created.Close()
			}
			return nil, fmt.Errorf("could not create consumer for subscription %q: %w", subscription, err)
		}

		// MaxDeliveries is 1-based (same scale as the Subscriber's
		// current_delivery), so it equals the broker's delivery cap before DLQ.
		// With no DLQ there is no cap at all — flag it so the Subscriber omits
		// max_deliveries rather than fabricating a finite bound.
		maxDeliveries, capped := 0, false
		if opt.DLQ != nil {
			maxDeliveries, capped = int(opt.DLQ.MaxDeliveries), true
		}

		consumers = append(consumers, pulsarConsumer{
			Consumer:      c,
			maxDeliveries: maxDeliveries,
			capped:        capped,
		})
	}

	r.mu.Lock()
	r.consumers = append(r.consumers, consumers...)
	r.mu.Unlock()

	return consumers, nil
}

func (r *ConsumerRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.consumers {
		c.Close()
	}
	return nil
}
