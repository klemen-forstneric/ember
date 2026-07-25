package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/klemen-forstneric/ember"
)

// releaseScript deletes the key only if it still holds our token, so a holder
// whose lock already expired (and was re-acquired by someone else) cannot
// delete the new holder's lock.
var releaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

const defaultLockTTL = 30 * time.Second

// Locker
type Locker struct {
	client redis.Cmdable
	ttl    time.Duration
}

func NewLocker(client redis.Cmdable, ttl time.Duration) *Locker {
	if ttl <= 0 {
		ttl = defaultLockTTL
	}
	return &Locker{client: client, ttl: ttl}
}

func (l *Locker) TryLock(ctx context.Context, key string) (ember.Lock, error) {
	current, err := l.token()
	if err != nil {
		return nil, err
	}

	args := redis.SetArgs{
		Mode: "NX",
		Get:  true,
		TTL:  l.ttl,
	}

	previous, err := l.client.SetArgs(ctx, key, current, args).Result()

	if errors.Is(err, redis.Nil) {
		return &lock{client: l.client, key: key, token: current}, nil
	} else if err != nil {
		return nil, err
	}

	if previous == current {
		return &lock{client: l.client, key: key, token: current}, nil
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
	token  string
}

func (l *lock) Release(ctx context.Context) error {
	return releaseScript.Run(ctx, l.client, []string{l.key}, l.token).Err()
}

var _ ember.Locker = (*Locker)(nil)
