package ext

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/klemen-forstneric/ember"
)

type mockSink struct{ mock.Mock }

func (m *mockSink) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

// mockLogger records level+msg per call so tests can assert logging without
// pinning down every variadic kv.
type mockLogger struct{ mock.Mock }

func (m *mockLogger) Debug(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Info(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Warn(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Error(ctx context.Context, msg string, err error, kvs ...interface{}) {
	m.Called(msg, err)
}
