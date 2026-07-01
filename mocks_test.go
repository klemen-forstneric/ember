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
