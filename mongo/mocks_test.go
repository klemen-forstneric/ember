package mongo

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/klemen-forstneric/ember"
)

type mockEventRepository struct{ mock.Mock }

func (m *mockEventRepository) ListUnpublished(ctx context.Context, limit int) ([]ember.EventEnvelope, error) {
	args := m.Called(ctx, limit)
	var envs []ember.EventEnvelope
	if v := args.Get(0); v != nil {
		envs = v.([]ember.EventEnvelope)
	}
	return envs, args.Error(1)
}

func (m *mockEventRepository) MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error {
	return m.Called(ctx, ids, expiresAt).Error(0)
}

type mockSink struct{ mock.Mock }

func (m *mockSink) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

type mockLocker struct{ mock.Mock }

func (m *mockLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (ember.Lock, error) {
	args := m.Called(ctx, key, ttl)
	var lock ember.Lock
	if v := args.Get(0); v != nil {
		lock = v.(ember.Lock)
	}
	return lock, args.Error(1)
}

type mockLock struct{ mock.Mock }

func (m *mockLock) Release(ctx context.Context) error { return m.Called(ctx).Error(0) }

// mockLogger records level+msg for each call so tests can assert on logging
// behavior without pinning down every variadic kv.
type mockLogger struct{ mock.Mock }

func (m *mockLogger) Debug(ctx context.Context, msg string, kvs ...interface{}) {
	m.Called(msg)
}

func (m *mockLogger) Info(ctx context.Context, msg string, kvs ...interface{}) {
	m.Called(msg)
}

func (m *mockLogger) Warn(ctx context.Context, msg string, kvs ...interface{}) {
	m.Called(msg)
}

func (m *mockLogger) Error(ctx context.Context, msg string, err error, kvs ...interface{}) {
	m.Called(msg, err)
}
