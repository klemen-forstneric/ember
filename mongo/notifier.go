package mongo

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/ext"
	"github.com/klemen-forstneric/ember/middleware"
)

// NotifierConfig tunes the relay. Zero values fall back to the defaults applied
// in NewNotifier. LockKey has no default and must be set (distinct per service
// when services share a database).
type NotifierConfig struct {
	IdleInterval time.Duration // idle poll cadence (jittered per replica)
	BatchSize    int           // events fetched per round
	LockKey      string        // redis efficiency-lock key
	LockTTL      time.Duration // lock lease; must exceed a bounded round
	Retention    time.Duration // published_at + Retention → expires_at (TTL)
}

// outboxStore is the slice of EventRepository the relay needs. *EventRepository
// satisfies it; tests supply a mock.
type outboxStore interface {
	ListUnpublished(ctx context.Context, limit int) ([]ember.EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}

// Notifier is the outbox delivery strategy: Notify is a no-op (persistence is
// the Publisher's repository); Run polls the store and publishes. Plug it into
// ember.NewPublisher's Notifier parameter.
type Notifier struct {
	store     outboxStore
	transport ext.Transport
	locker    middleware.Locker
	logger    ember.LoggerCtx
	cfg       NotifierConfig
}

func NewNotifier(store outboxStore, transport ext.Transport, locker middleware.Locker, logger ember.LoggerCtx, cfg NotifierConfig) *Notifier {
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
	return &Notifier{store: store, transport: transport, locker: locker, logger: logger, cfg: cfg}
}

// Notify satisfies ember.Notifier. Delivery is deferred to Run, so this is a
// no-op — persistence already happened via the Publisher's EventRepository.
func (n *Notifier) Notify(ctx context.Context, _ []ember.EventEnvelope) {}

// publishBatch fetches one batch of pending events and publishes them in seq
// order. On a publish failure it marks the event's entity failed and skips that
// entity's remaining events this batch (per-entity head-of-line), so a bad
// entity never blocks others and ordering is preserved. Returns the number of
// events marked published.
func (n *Notifier) publishBatch(ctx context.Context) (int, error) {
	events, err := n.store.ListUnpublished(ctx, n.cfg.BatchSize)
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
			n.logger.Error(ctx, "outbox publish failed", err,
				"event_id", e.ID, "entity_id", e.EntityID, "type", e.Event.Type)
			continue
		}
		published = append(published, e.ID)
	}

	if len(published) > 0 {
		expiresAt := time.Now().UTC().Add(n.cfg.Retention)
		if err := n.store.MarkPublished(ctx, published, expiresAt); err != nil {
			return len(published), err
		}
	}
	return len(published), nil
}

// tick acquires the efficiency lock and, if it wins, drains the backlog: it
// keeps publishing batches back-to-back as long as a full batch was published
// (more likely pending), then returns. If the lock is held elsewhere, the round
// is a no-op — only the winner touches mongo.
func (n *Notifier) tick(ctx context.Context) {
	lock, err := n.locker.TryLock(ctx, n.cfg.LockKey, n.cfg.LockTTL)
	if err != nil {
		n.logger.Error(ctx, "outbox lock error", err, "key", n.cfg.LockKey)
		return
	}
	if lock == nil {
		return // not leader this round
	}
	defer func() {
		if err := lock.Release(context.WithoutCancel(ctx)); err != nil {
			n.logger.Warn(ctx, "outbox lock release failed", "key", n.cfg.LockKey, "error", err)
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		published, err := n.publishBatch(ctx)
		if err != nil {
			n.logger.Error(ctx, "outbox batch error", err)
			return
		}
		if published < n.cfg.BatchSize {
			return // batch not fully drained (empty, short, or a failure) — wait for next tick
		}
	}
}

// Run drives the relay until ctx is cancelled: tick, then sleep a jittered idle
// interval. The jitter staggers replicas so their poll phases spread out,
// giving effective pickup latency well below the idle interval.
func (n *Notifier) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		n.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(n.interval()):
		}
	}
}

func (n *Notifier) interval() time.Duration {
	return n.cfg.IdleInterval + time.Duration(rand.Int64N(int64(n.cfg.IdleInterval)))
}

// Close is reserved for symmetry with other transports; the relay holds no
// resources between ticks, so shutdown is via cancelling Run's context.
func (n *Notifier) Close() error { return nil }
