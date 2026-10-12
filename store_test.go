package ember

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTypedStore(t *testing.T) {
	r := &mockTypedRepository[*fakeEntity]{}
	e := newFakeEntity("one")
	r.On("Get", mock.Anything, "one").Return(e, nil)
	r.On("Save", mock.Anything, e, e.Version().Inc()).Return(nil)
	p := NewPublisher(stubIDer{id: "event"}, NopMetadataGetter{}, &mockEventMarshaler{}, AtLeastOnce(&mockEventRepository{}))
	s := NewEntityStore(r, p, NopTransactor, nil)
	got, err := s.Get(context.Background(), "one")
	require.NoError(t, err)
	require.Same(t, e, got)
	require.NoError(t, s.Save(context.Background(), e))
	require.Equal(t, uint64(1), e.Version().Value())
	r.AssertExpectations(t)
}
