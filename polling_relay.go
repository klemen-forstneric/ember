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
	// PublishConcurrency caps how many entities of a round publish at once. An
	// entity's events always go out together, in order, in one Publish call.
	PublishConcurrency int
	// MaxHold bounds how long one partition's lock is held before it is released
	// and taken again. Keep it well below the Locker's TTL, or the lock can expire
	// while held and another relay republish the same rows.
	MaxHold time.Duration
}

const (
	defaultIdleInterval = 200 * time.Millisecond
	defaultBatchSize    = 500
	defaultRetention    = 7 * 24 * time.Hour
	defaultConcurrency  = 32
	defaultMaxHold      = 10 * time.Second
)

// DefaultPollingRelayConfig returns a PollingRelayConfig with sensible defaults.
// key must be unique per service and shared by that service's replicas.
func DefaultPollingRelayConfig(key string) PollingRelayConfig {
	return PollingRelayConfig{
		IdleInterval: defaultIdleInterval,
		BatchSize:    defaultBatchSize,
		LockKey:      key,
		Retention:    defaultRetention,

		PublishConcurrency: defaultConcurrency,
		MaxHold:            defaultMaxHold,
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
	case cfg.PublishConcurrency <= 0:
		return fmt.Errorf("%w: PublishConcurrency must be positive", ErrInvalidRelayConfig)
	case cfg.MaxHold <= 0:
		return fmt.Errorf("%w: MaxHold must be positive", ErrInvalidRelayConfig)
	}
	return nil
}

// ErrInvalidLimit is returned by ListUnpublished when limit is not positive.
var ErrInvalidLimit = errors.New("ember: limit must be positive")

// PollingRelayRepository is the drain side of a table-backed outbox. Rows are
// split into Partitions() partitions by entity (see Partition), so one entity's
// events stay in one partition and in order, and partitions drain independently.
type PollingRelayRepository interface {
	Partitions() int
	ListUnpublished(ctx context.Context, partition, limit int) ([]EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}

// Partition is the outbox partition of an entity among n; n below 2 means one partition.
func Partition(entityID string, n int) int {
	if n < 2 {
		return 0
	}
	return int(hashEntityID(entityID) % uint32(n))
}

// PollingRelay drains the outbox to the Sink. It is the sole publisher under the
// AtLeastOnce guarantee.
type PollingRelay struct {
	repository PollingRelayRepository
	sink       Sink
	locker     Locker
	logger     LoggerCtx
	cfg        PollingRelayConfig
	done       chan struct{}
	closeOnce  sync.Once
}

func NewPollingRelay(r PollingRelayRepository, s Sink, l Locker, log LoggerCtx, cfg PollingRelayConfig) (*PollingRelay, error) {
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

func (r *PollingRelay) publish(ctx context.Context, partition int) (int, error) {
	events, err := r.repository.ListUnpublished(ctx, partition, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}

	groups := make(map[string][]EventEnvelope)
	for _, e := range events {
		groups[e.EntityID] = append(groups[e.EntityID], e)
	}

	var (
		mu    sync.Mutex
		ids   = make([]string, 0, len(events))
		wg    sync.WaitGroup
		slots = make(chan struct{}, r.cfg.PublishConcurrency)
	)
	for id, es := range groups {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()

			if err := r.sink.Publish(ctx, es); err != nil {
				r.logger.Warn(ctx, "Failed to publish events, will retry",
					"error", err, "entity_id", id, "num_events", len(es))
				return
			}

			for _, e := range es {
				r.logger.Info(ctx, "Published event", "event_id", e.ID, "type", e.Event.Type,
					"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
					"metadata", e.Metadata, "timestamp", e.Timestamp,
					"lag", time.Since(e.Timestamp))
			}
			mu.Lock()
			for _, e := range es {
				ids = append(ids, e.ID)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(ids) == 0 {
		return 0, nil
	}

	expiresAt := time.Now().UTC().Add(r.cfg.Retention)
	if err := r.repository.MarkPublished(ctx, ids, expiresAt); err != nil {
		return 0, err
	}

	return len(ids), nil
}

// lockKey keeps the unpartitioned key for one partition, so existing deployments keep theirs.
func (r *PollingRelay) lockKey(partition int) string {
	if r.repository.Partitions() < 2 {
		return r.cfg.LockKey
	}
	return fmt.Sprintf("%s:%d", r.cfg.LockKey, partition)
}

// tick drains one partition while holding its lock, for at most MaxHold. It
// reports whether it stopped with rows possibly left, so the caller goes again
// without waiting.
func (r *PollingRelay) tick(ctx context.Context, partition int) bool {
	key := r.lockKey(partition)
	lock, err := r.locker.TryLock(ctx, key)
	if err != nil {
		r.logger.Error(ctx, "Failed to acquire outbox lock", err, "key", key)
		return false
	}
	if lock == nil {
		return false
	}

	defer func() {
		if err := lock.Release(context.WithoutCancel(ctx)); err != nil {
			r.logger.Warn(ctx, "Failed to release outbox lock", "key", key, "error", err)
		}
	}()

	deadline := time.Now().Add(r.cfg.MaxHold)
	for {
		if ctx.Err() != nil {
			return false
		}
		published, err := r.publish(ctx, partition)
		if err != nil {
			r.logger.Error(ctx, "Failed to drain outbox batch", err, "partition", partition)
			return false
		}
		if published == 0 {
			return false
		}
		if time.Now().After(deadline) {
			return true
		}
	}
}

// Run drains every partition, each in its own loop, until ctx ends or Close.
func (r *PollingRelay) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for p := range r.repository.Partitions() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runPartition(ctx, p)
		}()
	}
	wg.Wait()
}

func (r *PollingRelay) runPartition(ctx context.Context, partition int) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		default:
		}
		if r.tick(ctx, partition) {
			continue
		}
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
