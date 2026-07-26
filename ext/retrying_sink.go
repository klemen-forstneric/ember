package ext

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/klemen-forstneric/ember"
)

// RetryingSinkConfig
type RetryingSinkConfig struct {
	// InitialInterval is the delay before the second try.
	InitialInterval time.Duration
	// MaxInterval caps the delay between tries.
	MaxInterval time.Duration
	// MaxTries is the total number of attempts including the first. 1 disables retrying.
	MaxTries int
}

const (
	defaultInitialInterval = 100 * time.Millisecond
	defaultMaxInterval     = time.Second
	defaultMaxTries        = 3
)

// RetryingSink wraps a Sink with exponential-backoff retries.
//
// Under BestEffort it is the only retry there is. Wrapping PollingRelay's Sink
// stalls the outbox drain while its lock is held. Wrapping wal.Relay's Sink is
// fine — that relay keeps its replication connection alive across the call and
// retries the batch forever regardless, so MaxTries only sets how fast a
// transient failure is absorbed, not whether the events survive.
type RetryingSink struct {
	config RetryingSinkConfig
	sink   ember.Sink
	logger ember.LoggerCtx
}

func NewRetryingSink(c RetryingSinkConfig, s ember.Sink, l ember.LoggerCtx) *RetryingSink {
	if c.InitialInterval <= 0 {
		c.InitialInterval = defaultInitialInterval
	}
	if c.MaxInterval <= 0 {
		c.MaxInterval = defaultMaxInterval
	}
	if c.MaxTries <= 0 {
		c.MaxTries = defaultMaxTries
	}
	if l == nil {
		l = ember.NopLogger
	}

	return &RetryingSink{
		config: c,
		sink:   s,
		logger: l,
	}
}

var _ ember.Sink = (*RetryingSink)(nil)

func (r *RetryingSink) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	var try int

	publish := func() (struct{}, error) {
		try++
		return struct{}{}, r.sink.Publish(ctx, envelopes)
	}

	exp := backoff.NewExponentialBackOff()
	exp.InitialInterval = r.config.InitialInterval
	exp.MaxInterval = r.config.MaxInterval

	notify := func(err error, delay time.Duration) {
		r.logger.Warn(ctx, "Failed to publish events, retrying...",
			"error", err, "try", try, "delay", delay)
	}

	// WithMaxElapsedTime(0) disables v7's 15-minute default; MaxTries is the only bound.
	if _, err := backoff.Retry(ctx, publish,
		backoff.WithBackOff(exp),
		backoff.WithMaxTries(uint(r.config.MaxTries)), // defaulting guarantees >= 1
		backoff.WithMaxElapsedTime(0),
		backoff.WithNotify(notify),
	); err != nil {
		r.logger.Error(ctx, "Failed to publish events, tries exhausted", err, "tries", try)
		return err
	}

	for _, e := range envelopes {
		elapsed := time.Since(e.Timestamp)

		r.logger.Info(ctx, "Published event", "eventId", e.ID, "type", e.Event.Type,
			"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
			"metadata", e.Metadata, "timestamp", e.Timestamp,
			"elapsed_ms", elapsed.Milliseconds())
	}
	return nil
}
