package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

func TestTypedStoreListsThroughRepository(t *testing.T) {
	ctx := context.Background()
	r := &mockTypedRepository[*fakeEntity]{}
	e := newFakeEntity("one")
	f, sort := Eq("name", "one"), AscLex("id")
	page := Limit(2).After("previous", "previous")
	failure := errors.New("query failed")
	r.On("ListPage", ctx, f, sort, Unpaged()).Return([]*fakeEntity{e}, nil).Once()
	r.On("ListPage", ctx, f, sort, page).Return([]*fakeEntity{e}, nil).Once()
	r.On("ListPage", ctx, f, sort, Limit(3)).Return(nil, failure).Once()
	store := NewEntityStore(r, nil, NopTransactor, nil)
	got, err := store.List(ctx, f, sort)
	require.NoError(t, err)
	require.Equal(t, []*fakeEntity{e}, got)
	got, err = store.ListPage(ctx, f, sort, page)
	require.NoError(t, err)
	require.Equal(t, []*fakeEntity{e}, got)
	_, err = store.ListPage(ctx, f, sort, Limit(3))
	require.ErrorIs(t, err, failure)
	r.AssertExpectations(t)
}
