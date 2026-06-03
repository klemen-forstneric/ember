package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// defaultRetryBackoff is the in-session re-emit delay applied to a nacked
// message when a subscription does not configure its own RetryBackoff.
const defaultRetryBackoff = 500 * time.Millisecond

// reader is the slice of *kafka.Reader the Subscriber needs, plus the
// per-subscription delivery cap and retry backoff. The registry interprets the
// SubscriptionConfig and supplies these, so the Subscriber's engine reads them
// off the reader without depending on the registry's config type.
type reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
	MaxDeliveries() (int, bool) // (cap, capped); capped=false => no cap
	RetryBackoff() time.Duration
}

// kafkaReader adapts a *kafka.Reader to the reader interface by embedding it
// (promoting FetchMessage/CommitMessages/Close) and adding the config-derived
// MaxDeliveries and RetryBackoff.
type kafkaReader struct {
	*kafka.Reader
	maxDeliveries int
	capped        bool
	backoff       time.Duration
}

func (r kafkaReader) MaxDeliveries() (int, bool)  { return r.maxDeliveries, r.capped }
func (r kafkaReader) RetryBackoff() time.Duration { return r.backoff }

// SubscriptionConfig describes how one subscription consumes from Kafka. Run
// replicas with the same GroupID to scale: Kafka distributes the subscription's
// partitions across them.
type SubscriptionConfig struct {
	GroupID       string        // defaults to the subscription name when empty
	Topics        []string      // one or more topics for this subscription
	MaxDeliveries int           // 0 => uncapped (capped=false)
	RetryBackoff  time.Duration // 0 => defaultRetryBackoff
}

// ConsumerRegistry maps a subscription name to a single consumer-group reader.
type ConsumerRegistry struct {
	brokers []string
	config  map[string]SubscriptionConfig

	mu      sync.Mutex
	readers []reader
}

func NewConsumerRegistry(brokers []string, config map[string]SubscriptionConfig) *ConsumerRegistry {
	return &ConsumerRegistry{brokers: brokers, config: config}
}

// Get ignores ctx: kafka.NewReader takes only a config. The parameter is kept
// for interface symmetry.
func (r *ConsumerRegistry) Get(_ context.Context, subscription string) (reader, error) {
	cfg, ok := r.config[subscription]
	if !ok {
		return nil, fmt.Errorf("no consumer config for subscription %q", subscription)
	}

	groupID := cfg.GroupID
	if groupID == "" {
		groupID = subscription
	}

	rc := kafka.ReaderConfig{Brokers: r.brokers, GroupID: groupID}
	// A consumer-group reader takes Topic for a single topic or GroupTopics for
	// several; setting both (or neither) panics in kafka.NewReader, so pick
	// exactly one and reject an empty topic list with a clear error.
	switch len(cfg.Topics) {
	case 0:
		return nil, fmt.Errorf("subscription %q: Topics must not be empty", subscription)
	case 1:
		rc.Topic = cfg.Topics[0]
	default:
		rc.GroupTopics = cfg.Topics
	}

	backoff := cfg.RetryBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}

	kr := kafkaReader{
		Reader:        kafka.NewReader(rc),
		maxDeliveries: cfg.MaxDeliveries,
		capped:        cfg.MaxDeliveries > 0,
		backoff:       backoff,
	}

	r.mu.Lock()
	r.readers = append(r.readers, kr)
	r.mu.Unlock()

	return kr, nil
}

func (r *ConsumerRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, rd := range r.readers {
		if err := rd.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
