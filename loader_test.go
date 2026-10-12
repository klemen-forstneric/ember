package ember

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTypedLoader(t *testing.T) {
	r := &mockTypedRepository[*fakeEntity]{}
	e := newFakeEntity("one")
	r.On("Get", mock.Anything, "one").Return(e, nil)
	r.On("Get", mock.Anything, "missing").Return(nil, ErrEntityNotFound)
	l := NewEntityLoader(Bind(r))
	got, err := l.Get(context.Background(), "one")
	require.NoError(t, err)
	require.Same(t, e, got)
	_, err = l.Get(context.Background(), "missing")
	require.ErrorIs(t, err, ErrEntityNotFound)
	r.AssertExpectations(t)
}
