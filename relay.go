package ember

import (
	"context"
	"encoding/json"
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

func NewRelay(r EventRepository, s Sink, l Locker, log LoggerCtx, cfg RelayConfig) *Relay {
	if cfg.IdleInterval <= 0 {
		cfg.IdleInterval = 200 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = 30 * time.Second
	}
	if cfg.Retention <= 0 {
		cfg.Retention = 7 * 24 * time.Hour
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
	}
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	events, err := r.repository.ListUnpublished(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}

	failed := make(map[string]bool)
	published := make([]string, 0, len(events))
	for i := range events {
		e := events[i]
		if failed[e.EntityID] {
			continue
		}
		if err := r.sink.Publish(ctx, []EventEnvelope{e}); err != nil {
			failed[e.EntityID] = true
			r.logger.Warn(ctx, "Failed to publish event, will retry",
				"error", err, "eventId", e.ID, "type", e.Event.Type, "entity_id", e.EntityID)
			continue
		}
		published = append(published, e.ID)

		elapsed := time.Since(e.Timestamp)
		r.logger.Info(ctx, "Published event", "eventId", e.ID, "type", e.Event.Type,
			"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
			"metadata", e.Metadata, "timestamp", e.Timestamp,
			"elapsed_ms", elapsed.Milliseconds())
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
