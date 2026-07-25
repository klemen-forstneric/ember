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

// RelayConfig
type RelayConfig struct {
	IdleInterval time.Duration // idle poll cadence (jittered per replica)
	BatchSize    int           // events fetched per round
	LockKey      string        // redis efficiency-lock key
	LockTTL      time.Duration // lock lease; must exceed a bounded round
	Retention    time.Duration // published_at + Retention → expires_at (TTL)
}

const (
	defaultIdleInterval = 200 * time.Millisecond
	defaultBatchSize    = 500
	defaultLockTTL      = 30 * time.Second
	defaultRetention    = 7 * 24 * time.Hour
)

// DefaultRelayConfig returns a RelayConfig with sensible defaults. key must be
// unique per service and shared by that service's replicas.
func DefaultRelayConfig(key string) RelayConfig {
	return RelayConfig{
		IdleInterval: defaultIdleInterval,
		BatchSize:    defaultBatchSize,
		LockKey:      key,
		LockTTL:      defaultLockTTL,
		Retention:    defaultRetention,
	}
}

// ErrInvalidRelayConfig is returned by NewRelay when cfg fails validation.
var ErrInvalidRelayConfig = errors.New("ember: invalid relay config")

func validateRelayConfig(cfg RelayConfig) error {
	switch {
	case cfg.LockKey == "":
		return fmt.Errorf("%w: LockKey must not be empty", ErrInvalidRelayConfig)
	case cfg.IdleInterval <= 0:
		return fmt.Errorf("%w: IdleInterval must be positive", ErrInvalidRelayConfig)
	case cfg.BatchSize <= 0:
		return fmt.Errorf("%w: BatchSize must be positive", ErrInvalidRelayConfig)
	case cfg.LockTTL <= 0:
		return fmt.Errorf("%w: LockTTL must be positive", ErrInvalidRelayConfig)
	case cfg.Retention <= 0:
		return fmt.Errorf("%w: Retention must be positive", ErrInvalidRelayConfig)
	case cfg.LockTTL <= cfg.IdleInterval:
		return fmt.Errorf("%w: LockTTL must exceed IdleInterval", ErrInvalidRelayConfig)
	}
	return nil
}

// Relay drains the outbox to the Sink. It is the sole publisher under the
// AtLeastOnce guarantee.
type Relay struct {
	repository EventRepository
	sink       Sink
	locker     Locker
	logger     LoggerCtx
	cfg        RelayConfig
	done       chan struct{}
	closeOnce  sync.Once
}

func NewRelay(r EventRepository, s Sink, l Locker, log LoggerCtx, cfg RelayConfig) (*Relay, error) {
	if err := validateRelayConfig(cfg); err != nil {
		return nil, err
	}
	if log == nil {
		log = NopLogger
	}

	return &Relay{
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

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	events, err := r.repository.ListUnpublished(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}

	published := make([]string, 0, len(events))
	for _, group := range groupByEntity(events) {
		n, err := r.sink.Publish(ctx, group)
		for _, e := range group[:n] {
			published = append(published, e.ID)

			elapsed := time.Since(e.Timestamp)
			r.logger.Info(ctx, "Published event", "eventId", e.ID, "type", e.Event.Type,
				"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
				"metadata", e.Metadata, "timestamp", e.Timestamp,
				"elapsed_ms", elapsed.Milliseconds())
		}
		if err != nil && n < len(group) {
			e := group[n]
			r.logger.Warn(ctx, "Failed to publish event, will retry",
				"error", err, "eventId", e.ID, "type", e.Event.Type, "entity_id", e.EntityID)
		}
	}

	if len(published) > 0 {
		expiresAt := time.Now().UTC().Add(r.cfg.Retention)
		if err := r.repository.MarkPublished(ctx, published, expiresAt); err != nil {
			return len(published), err
		}
	}
	return len(published), nil
}

func (r *Relay) tick(ctx context.Context) {
	lock, err := r.locker.TryLock(ctx, r.cfg.LockKey, r.cfg.LockTTL)
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
		published, err := r.publishBatch(ctx)
		if err != nil {
			r.logger.Error(ctx, "Failed to drain outbox batch", err)
			return
		}
		if published < r.cfg.BatchSize {
			return
		}
	}
}

func (r *Relay) Run(ctx context.Context) {
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

func (r *Relay) interval() time.Duration {
	return r.cfg.IdleInterval + time.Duration(rand.Int64N(int64(r.cfg.IdleInterval)))
}

func (r *Relay) Close() error {
	r.closeOnce.Do(func() { close(r.done) })
	return nil
}
