package ext

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/klemen-forstneric/ember"
)

// RetryingeNotifierConfig
type RetryingeNotifierConfig struct {
	// InitialInterval is the delay for the first retry.
	InitialInterval time.Duration
	// MaxInterval is the maximum delay between retries.
	MaxInterval time.Duration
	// MaxElapsedTime is the time after which we stop retrying.
	MaxElapsedTime time.Duration
}

type Transport interface {
	Publish(ctx context.Context, envelopes []ember.EventEnvelope) error
}

type RetryingNotifier struct {
	config    RetryingeNotifierConfig
	transport Transport
	logger    ember.LoggerCtx
}

func NewRetryingNotifier(c RetryingeNotifierConfig, t Transport, l ember.LoggerCtx) *RetryingNotifier {
	if c.MaxElapsedTime == 0 {
		// With -1 we disable indefinite retries.
		c.MaxElapsedTime = -1
	}

	return &RetryingNotifier{
		config:    c,
		transport: t,
		logger:    l,
	}
}

func (n *RetryingNotifier) Notify(ctx context.Context, envelopes []ember.EventEnvelope) {
	var attempt int

	publish := func() error {
		attempt++
		return n.transport.Publish(ctx, envelopes)
	}

	b := backoff.NewExponentialBackOff()
	b.InitialInterval = n.config.InitialInterval
	b.MaxInterval = n.config.MaxInterval
	b.MaxElapsedTime = n.config.MaxElapsedTime

	notify := func(err error, delay time.Duration) {
		n.logger.Warn(ctx, "Failed to publish events, retrying...",
			"error", err, "attempt", attempt, "delay", delay)
	}

	if err := backoff.RetryNotify(publish, b, notify); err != nil {
		n.logger.Error(ctx, "Failed to publish events, retries exhausted", err)
		return
	}

	for _, e := range envelopes {
		elapsed := time.Since(e.Timestamp)

		n.logger.Info(ctx, "Published event", "eventId", e.ID, "type", e.Event.Type,
			"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
			"metadata", e.Metadata, "timestamp", e.Timestamp,
			"elapsed_ms", elapsed.Milliseconds())
	}
}
