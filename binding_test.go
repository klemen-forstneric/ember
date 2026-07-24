package ember

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBindingReportsTypeAndMarshals(t *testing.T) {
	repo := &mockEntityRepository{}
	marshaler := &mockEntityMarshaler[*fakeEntity]{}
	b := Bind[*fakeEntity](repo, marshaler).binding()

	require.Equal(t, "fake", b.typ) // fakeEntity.Type() == "fake"

	e := newFakeEntity("1")
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	marshaler.On("Marshal", mock.Anything, e).Return(m, nil)
	got, err := b.marshal(context.Background(), e)
	require.NoError(t, err)
	require.Same(t, m, got)
	marshaler.AssertExpectations(t)
}
