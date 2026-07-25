package ember

import (
	"context"
	"time"
)

// Lock
type Lock interface {
	Release(ctx context.Context) error
}

// Locker
type Locker interface {
	TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}
