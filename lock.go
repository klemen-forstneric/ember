package ember

import (
	"context"
)

// Lock
type Lock interface {
	Release(ctx context.Context) error
}

// Locker
type Locker interface {
	TryLock(ctx context.Context, key string) (Lock, error)
}
