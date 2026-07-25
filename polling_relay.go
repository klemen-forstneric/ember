package ember

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// PollingRelayConfig
type PollingRelayConfig struct {
	IdleInterval time.Duration // idle poll cadence (jittered per replica)
	BatchSize    int           // events fetched per round
	LockKey      string        // redis efficiency-lock key
	Retention    time.Duration // published_at + Retention → expires_at (TTL)
}

const (
	defaultIdleInterval = 200 * time.Millisecond
	defaultBatchSize    = 500
	defaultRetention    = 7 * 24 * time.Hour
)

// DefaultPollingRelayConfig returns a PollingRelayConfig with sensible defaults.
// key must be unique per service and shared by that service's replicas.
func DefaultPollingRelayConfig(key string) PollingRelayConfig {
	return PollingRelayConfig{
		IdleInterval: defaultIdleInterval,
		BatchSize:    defaultBatchSize,
		LockKey:      key,
		Retention:    defaultRetention,
	}
}

// ErrInvalidRelayConfig is returned by NewRelay when cfg fails validation.
var ErrInvalidRelayConfig = errors.New("ember: invalid relay config")

func validateRelayConfig(cfg PollingRelayConfig) error {
	switch {
	case cfg.LockKey == "":
		return fmt.Errorf("%w: LockKey must not be empty", ErrInvalidRelayConfig)
	case cfg.IdleInterval <= 0:
		return fmt.Errorf("%w: IdleInterval must be positive", ErrInvalidRelayConfig)
	case cfg.BatchSize <= 0:
		return fmt.Errorf("%w: BatchSize must be positive", ErrInvalidRelayConfig)
	case cfg.Retention <= 0:
		return fmt.Errorf("%w: Retention must be positive", ErrInvalidRelayConfig)
	}
	return nil
}

// PollingRelay drains the outbox to the Sink. It is the sole publisher under the
// AtLeastOnce guarantee.
type PollingRelay struct {
	repository EventRepository
	sink       Sink
	locker     Locker
	logger     LoggerCtx
	cfg        PollingRelayConfig
	done       chan struct{}
	closeOnce  sync.Once
}

func NewPollingRelay(r EventRepository, s Sink, l Locker, log LoggerCtx, cfg PollingRelayConfig) (*PollingRelay, error) {
	if err := validateRelayConfig(cfg); err != nil {
		return nil, err
	}
	if log == nil {
		log = NopLogger
	}

	return &PollingRelay{
		repository: r,
		sink:       s,
		locker:     l,
		logger:     log,
		cfg:        cfg,
		done:       make(chan struct{}),
	}, nil
}

// groupByEntity partitions events into per-entity runs, preserving each
// entity's internal order and ordering groups by first appearance.
func groupByEntity(events []EventEnvelope) [][]EventEnvelope {
	index := make(map[string]int, len(events))
	groups := make([][]EventEnvelope, 0, len(events))
	for _, e := range events {
		i, ok := index[e.EntityID]
		if !ok {
			index[e.EntityID] = len(groups)
			groups = append(groups, []EventEnvelope{e})
			continue
		}
		groups[i] = append(groups[i], e)
	}
	return groups
}

func (r *PollingRelay) publish(ctx context.Context) (int, error) {
	events, err := r.repository.ListUnpublished(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}

	ids := make([]string, 0, len(events))

	for _, g := range groupByEntity(events) {
		if err := r.sink.Publish(ctx, g); err != nil {
			r.logger.Warn(ctx, "Failed to publish events, will retry",
				"error", err, "entity_id", g[0].EntityID, "events", len(g))
			continue
		}

		for _, e := range g {
			ids = append(ids, e.ID)

			elapsed := time.Since(e.Timestamp)
			r.logger.Info(ctx, "Published event", "eventId", e.ID, "type", e.Event.Type,
				"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
				"metadata", e.Metadata, "timestamp", e.Timestamp,
				"elapsed_ms", elapsed.Milliseconds())
		}
	}

	if len(ids) == 0 {
		return 0, nil
	}

	expiresAt := time.Now().UTC().Add(r.cfg.Retention)
	if err := r.repository.MarkPublished(ctx, ids, expiresAt); err != nil {
		return 0, err
	}

	return len(ids), nil
}

func (r *PollingRelay) tick(ctx context.Context) {
	lock, err := r.locker.TryLock(ctx, r.cfg.LockKey)
	if err != nil {
		r.logger.Error(ctx, "Failed to acquire outbox lock", err, "key", r.cfg.LockKey)
		return
	}
	if lock == nil {
		return
	}

	defer func() {
		if err := lock.Release(context.WithoutCancel(ctx)); err != nil {
			r.logger.Warn(ctx, "Failed to release outbox lock", "key", r.cfg.LockKey, "error", err)
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		published, err := r.publish(ctx)
		if err != nil {
			r.logger.Error(ctx, "Failed to drain outbox batch", err)
			return
		}
		if published < r.cfg.BatchSize {
			return
		}
	}
}

func (r *PollingRelay) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		default:
		}
		r.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-time.After(r.interval()):
		}
	}
}

func (r *PollingRelay) interval() time.Duration {
	return r.cfg.IdleInterval + time.Duration(rand.Int64N(int64(r.cfg.IdleInterval)))
}

func (r *PollingRelay) Close() error {
	r.closeOnce.Do(func() { close(r.done) })
	return nil
}
