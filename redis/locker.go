package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/klemen-forstneric/ember/middleware"
)

// Locker
type Locker struct {
	client redis.Cmdable
}

func NewLocker(client redis.Cmdable) *Locker {
	return &Locker{client: client}
}

func (l *Locker) TryLock(ctx context.Context, key string, ttl time.Duration) (middleware.Lock, error) {
	current, err := l.token()
	if err != nil {
		return nil, err
	}

	args := redis.SetArgs{
		Mode: "NX",
		Get:  true,
		TTL:  ttl,
	}

	previous, err := l.client.SetArgs(ctx, key, current, args).Result()

	if errors.Is(err, redis.Nil) {
		return &lock{client: l.client, key: key}, nil
	} else if err != nil {
		return nil, err
	}

	if previous == current {
		return &lock{client: l.client, key: key}, nil
	}

	return nil, nil
}

func (l *Locker) token() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// lock
type lock struct {
	client redis.Cmdable
	key    string
}

func (l *lock) Release(ctx context.Context) error {
	return l.client.Del(ctx, l.key).Err()
}
