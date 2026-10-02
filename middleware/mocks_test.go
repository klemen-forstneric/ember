package middleware_test

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/klemen-forstneric/ember"
)

// mockLocker
type mockLocker struct {
	mock.Mock
}

func (m *mockLocker) TryLock(ctx context.Context, key string) (ember.Lock, error) {
	args := m.Called(ctx, key)
	var out ember.Lock
	if v := args.Get(0); v != nil {
		out = v.(ember.Lock)
	}
	return out, args.Error(1)
}

// mockLock
type mockLock struct {
	mock.Mock
}

func (m *mockLock) Release(ctx context.Context) error {
	return m.Called(ctx).Error(0)
}

// mockLogger
type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) Debug(context.Context, string, ...interface{}) {}
func (m *mockLogger) Info(context.Context, string, ...interface{})  {}
func (m *mockLogger) Warn(context.Context, string, ...interface{})  {}

func (m *mockLogger) Error(ctx context.Context, msg string, err error, kvs ...interface{}) {
	m.Called(ctx, msg, err, kvs)
}
