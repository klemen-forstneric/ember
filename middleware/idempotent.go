package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/klemen-forstneric/ember"
)

var ErrLockerUnavailable = errors.New("ember/middleware: locker unavailable")

// Lock
type Lock interface {
	Release(ctx context.Context) error
}

// Locker
type Locker interface {
	TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}

func Idempotent(keyPrefix string, ttl time.Duration, locker Locker, l ember.LoggerCtx) ember.SubscriptionMiddleware {
	return func(next ember.HandleFunc) ember.HandleFunc {
		return func(ctx context.Context, e *ember.ReceivedEvent) error {
			key := keyPrefix + "_" + e.ID

			lock, err := locker.TryLock(ctx, key, ttl)
			if err != nil {
				l.Error(ctx, "Failed to acquire idempotency lock", err, "event_id", e.ID, "key", key)
				return ErrLockerUnavailable
			}

			if lock == nil {
				l.Info(ctx, "Skipping already handled event", "event_id", e.ID, "key", key)
				return nil
			}

			err = next(ctx, e)
			if err == nil {
				return nil
			}

			ctx = context.WithoutCancel(ctx)
			if relErr := lock.Release(ctx); relErr != nil {
				l.Warn(ctx, "Failed to release idempotency lock", "event_id", e.ID,
					"key", key, "error", relErr)
			}

			return err
		}
	}
}
