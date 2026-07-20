package mongo

import (
	"context"
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
