package ember

import (
	"context"

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
}

func (m *mockTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	args := m.Called(ctx)
	if err := fn(ctx); err != nil {
		return err
	}
	return args.Error(0)
}
