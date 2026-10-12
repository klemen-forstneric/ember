package ember

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBindingReportsTypeAndSaves(t *testing.T) {
	r := &mockTypedRepository[*fakeEntity]{}
	e := newFakeEntity("one")
	v := NewVersion(0).Inc()
	r.On("Save", mock.Anything, e, v).Return(nil).Run(func(mock.Arguments) {
		require.Equal(t, uint64(0), e.Version().Value(), "adapter gets proposed version separately")
	})
	b := Bind(r).binding()
	require.Equal(t, "fake", b.typ)
	require.NoError(t, b.save(context.Background(), e, v))
	r.AssertExpectations(t)
}
