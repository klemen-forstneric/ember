package mongo

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/middleware"
)

type mockStore struct{ mock.Mock }

func (m *mockStore) ListUnpublished(ctx context.Context, limit int) ([]ember.EventEnvelope, error) {
	args := m.Called(ctx, limit)
	var envs []ember.EventEnvelope
	if v := args.Get(0); v != nil {
		envs = v.([]ember.EventEnvelope)
	}
	return envs, args.Error(1)
}

func (m *mockStore) MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error {
	return m.Called(ctx, ids, expiresAt).Error(0)
}

type mockTransport struct{ mock.Mock }

func (m *mockTransport) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

type mockLocker struct{ mock.Mock }

func (m *mockLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (middleware.Lock, error) {
	args := m.Called(ctx, key, ttl)
	var lock middleware.Lock
	if v := args.Get(0); v != nil {
		lock = v.(middleware.Lock)
	}
	return lock, args.Error(1)
}

type mockLock struct{ mock.Mock }

func (m *mockLock) Release(ctx context.Context) error { return m.Called(ctx).Error(0) }
