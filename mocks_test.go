package ember

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

// mockEntityRepository is a testify mock for EntityRepository.
type mockEntityRepository struct {
	mock.Mock
}

func (m *mockEntityRepository) Save(ctx context.Context, me *MarshaledEntity) error {
	return m.Called(ctx, me).Error(0)
}

func (m *mockEntityRepository) Get(ctx context.Context, typ, id string) (*MarshaledEntity, error) {
	args := m.Called(ctx, typ, id)
	var out *MarshaledEntity
	if v := args.Get(0); v != nil {
		out = v.(*MarshaledEntity)
	}
	return out, args.Error(1)
}

func (m *mockEntityRepository) List(ctx context.Context, typ string, f Filter, s Sort) ([]*MarshaledEntity, error) {
	args := m.Called(ctx, typ, f, s)
	var out []*MarshaledEntity
	if v := args.Get(0); v != nil {
		out = v.([]*MarshaledEntity)
	}
	return out, args.Error(1)
}

// mockEntityMarshaler is a testify mock for EntityMarshaler.
type mockEntityMarshaler[E Entity] struct {
	mock.Mock
}

func (m *mockEntityMarshaler[E]) Marshal(ctx context.Context, e E) (*MarshaledEntity, error) {
	args := m.Called(ctx, e)
	var out *MarshaledEntity
	if v := args.Get(0); v != nil {
		out = v.(*MarshaledEntity)
	}
	return out, args.Error(1)
}

func (m *mockEntityMarshaler[E]) Unmarshal(ctx context.Context, me *MarshaledEntity) (E, error) {
	args := m.Called(ctx, me)
	var out E
	if v := args.Get(0); v != nil {
		out = v.(E)
	}
	return out, args.Error(1)
}

// mockEventRepository is a testify mock for EventRepository.
type mockEventRepository struct {
	mock.Mock
}

func (m *mockEventRepository) Save(ctx context.Context, envelopes []EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

func (m *mockEventRepository) ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error) {
	args := m.Called(ctx, limit)
	var envs []EventEnvelope
	if v := args.Get(0); v != nil {
		envs = v.([]EventEnvelope)
	}
	return envs, args.Error(1)
}

func (m *mockEventRepository) MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error {
	return m.Called(ctx, ids, expiresAt).Error(0)
}

// mockEventMarshaler is a testify mock for EventMarshaler.
type mockEventMarshaler struct {
	mock.Mock
}

func (m *mockEventMarshaler) Marshal(ctx context.Context, e Event) (*MarshaledEvent, error) {
	args := m.Called(ctx, e)
	var out *MarshaledEvent
	if v := args.Get(0); v != nil {
		out = v.(*MarshaledEvent)
	}
	return out, args.Error(1)
}

func (m *mockEventMarshaler) Unmarshal(ctx context.Context, e *MarshaledEvent) (Event, error) {
	args := m.Called(ctx, e)
	var out Event
	if v := args.Get(0); v != nil {
		out = v.(Event)
	}
	return out, args.Error(1)
}

// stubIDer returns a fixed id (event envelope IDs are not under test here).
type stubIDer struct{ id string }

func (s stubIDer) ID() string { return s.id }

// mockTransactor runs the callback (as a real transaction boundary does) and
// returns the handler's error, else the configured transaction-level error.
type mockTransactor struct {
	mock.Mock
	inTx bool
}

func (m *mockTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	args := m.Called(ctx)
	if err := fn(ctx); err != nil {
		return err
	}
	return args.Error(0)
}

func (m *mockTransactor) InTx(context.Context) bool { return m.inTx }

// mockSink is a testify mock for Sink.
type mockSink struct {
	mock.Mock
}

func (m *mockSink) Publish(ctx context.Context, envelopes []EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

// recordingTransactor marks the commit boundary so tests can assert that a
// deferred delivery runs after it, which a mock's call order cannot express.
type recordingTransactor struct {
	committed bool
	inTx      bool
}

func (t *recordingTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	if err := fn(ctx); err != nil {
		return err
	}
	t.committed = true
	return nil
}

func (t *recordingTransactor) InTx(context.Context) bool { return t.inTx }

// cancelAfterCommitTransactor runs fn, then cancels the ctx's own cancel func,
// simulating a client disconnect that lands right after commit.
type cancelAfterCommitTransactor struct {
	cancel context.CancelFunc
	inTx   bool
}

func (t *cancelAfterCommitTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	if err := fn(ctx); err != nil {
		return err
	}
	t.cancel()
	return nil
}

func (t *cancelAfterCommitTransactor) InTx(context.Context) bool { return t.inTx }

// mockLocker is a testify mock for Locker.
type mockLocker struct {
	mock.Mock
}

func (m *mockLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error) {
	args := m.Called(ctx, key, ttl)
	var lock Lock
	if v := args.Get(0); v != nil {
		lock = v.(Lock)
	}
	return lock, args.Error(1)
}

// mockLock is a testify mock for Lock.
type mockLock struct {
	mock.Mock
}

func (m *mockLock) Release(ctx context.Context) error { return m.Called(ctx).Error(0) }

// mockLogger records level+msg per call so tests can assert logging without
// pinning down every variadic kv.
type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) Debug(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Info(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Warn(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Error(ctx context.Context, msg string, err error, kvs ...interface{}) {
	m.Called(msg, err)
}
