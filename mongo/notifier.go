package mongo

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/ext"
	"github.com/klemen-forstneric/ember/middleware"
)

// NotifierConfig
type NotifierConfig struct {
	IdleInterval time.Duration // idle poll cadence (jittered per replica)
	BatchSize    int           // events fetched per round
	LockKey      string        // redis efficiency-lock key
	LockTTL      time.Duration // lock lease; must exceed a bounded round
	Retention    time.Duration // published_at + Retention → expires_at (TTL)
}

// eventRepository
type eventRepository interface {
	ListUnpublished(ctx context.Context, limit int) ([]ember.EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}

// Notifier
type Notifier struct {
	repository eventRepository
	transport  ext.Transport
	locker     middleware.Locker
	logger     ember.LoggerCtx
	cfg        NotifierConfig
	done       chan struct{}
	closeOnce  sync.Once
}

func NewNotifier(store eventRepository, transport ext.Transport, locker middleware.Locker, logger ember.LoggerCtx, cfg NotifierConfig) *Notifier {
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
	if logger == nil {
		logger = ember.NopLogger
	}

	return &Notifier{
		repository: store,
		transport:  transport,
		locker:     locker,
		logger:     logger,
		cfg:        cfg,
		done:       make(chan struct{}),
	}
}

func (n *Notifier) Notify(ctx context.Context, _ []ember.EventEnvelope) {
}

func (n *Notifier) publishBatch(ctx context.Context) (int, error) {
	events, err := n.repository.ListUnpublished(ctx, n.cfg.BatchSize)
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
		if err := n.transport.Publish(ctx, []ember.EventEnvelope{e}); err != nil {
			failed[e.EntityID] = true
			n.logger.Warn(ctx, "Failed to publish event, will retry",
				"error", err, "eventId", e.ID, "type", e.Event.Type, "entity_id", e.EntityID)
			continue
		}
		published = append(published, e.ID)

		elapsed := time.Since(e.Timestamp)
		n.logger.Info(ctx, "Published event", "eventId", e.ID, "type", e.Event.Type,
			"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
			"metadata", e.Metadata, "timestamp", e.Timestamp,
			"elapsed_ms", elapsed.Milliseconds())
	}

	if len(published) > 0 {
		expiresAt := time.Now().UTC().Add(n.cfg.Retention)
		if err := n.repository.MarkPublished(ctx, published, expiresAt); err != nil {
			return len(published), err
		}
	}
	return len(published), nil
}

func (n *Notifier) tick(ctx context.Context) {
	lock, err := n.locker.TryLock(ctx, n.cfg.LockKey, n.cfg.LockTTL)
	if err != nil {
		n.logger.Error(ctx, "Failed to acquire outbox lock", err, "key", n.cfg.LockKey)
		return
	}
	if lock == nil {
		return
	}

	defer func() {
		if err := lock.Release(context.WithoutCancel(ctx)); err != nil {
			n.logger.Warn(ctx, "Failed to release outbox lock", "key", n.cfg.LockKey, "error", err)
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		published, err := n.publishBatch(ctx)
		if err != nil {
			n.logger.Error(ctx, "Failed to drain outbox batch", err)
			return
		}
		if published < n.cfg.BatchSize {
			return
		}
	}
}

func (n *Notifier) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.done:
			return
		default:
		}
		n.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-n.done:
			return
		case <-time.After(n.interval()):
		}
	}
}

func (n *Notifier) interval() time.Duration {
	return n.cfg.IdleInterval + time.Duration(rand.Int64N(int64(n.cfg.IdleInterval)))
}

func (n *Notifier) Close() error {
	n.closeOnce.Do(func() { close(n.done) })
	return nil
}
