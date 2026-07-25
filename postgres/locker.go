package postgres

import (
	"context"
	"database/sql"
	"errors"
	"hash/fnv"
	"sync"

	"github.com/klemen-forstneric/ember"
)

// Locker takes postgres session-scoped advisory locks. Each lock holds its own
// pooled connection, since the session that acquires an advisory lock is the
// only one that can release it. Advisory locks have no lease: there is no
// mid-round expiry, and a hard crash frees the lock only once postgres
// notices the dead session.
type Locker struct {
	pool *sql.DB
}

func NewLocker(pool *sql.DB) *Locker {
	return &Locker{pool: pool}
}

var _ ember.Locker = (*Locker)(nil)

func (l *Locker) TryLock(ctx context.Context, key string) (ember.Lock, error) {
	conn, err := l.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}

	id := lockID(key)

	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", id).Scan(&acquired); err != nil {
		conn.Close()
		return nil, err
	}
	if !acquired {
		conn.Close()
		return nil, nil
	}

	return &lock{conn: conn, id: id}, nil
}

// lockID maps key to the bigint advisory locks take. Advisory lock keys are
// global to the database, so an unrelated application using the same integer
// would collide.
func lockID(key string) int64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int64(h.Sum64())
}

type lock struct {
	conn *sql.Conn
	id   int64
	once sync.Once
}

func (l *lock) Release(ctx context.Context) error {
	var err error
	l.once.Do(func() {
		_, unlock := l.conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", l.id)
		// Close returns the conn to the pool rather than ending the session; if
		// unlock failed this at least discards it on next use, ending the session.
		err = errors.Join(unlock, l.conn.Close())
	})
	return err
}
