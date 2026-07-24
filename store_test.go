package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type StoreSuite struct {
	suite.Suite
	ctx           context.Context
	entityRepo    *mockEntityRepository
	entityMarshal *mockEntityMarshaler[*fakeEntity]
	eventRepo     *mockEventRepository
	eventMarshal  *mockEventMarshaler
	tx            *mockTransactor
	store         *Store[*fakeEntity]
}

func TestStoreSuite(t *testing.T) {
	suite.Run(t, new(StoreSuite))
}

func (s *StoreSuite) SetupTest() {
	s.ctx = context.Background()
	s.entityRepo = &mockEntityRepository{}
	s.entityMarshal = &mockEntityMarshaler[*fakeEntity]{}
	s.eventRepo = &mockEventRepository{}
	s.eventMarshal = &mockEventMarshaler{}
	s.tx = &mockTransactor{}

	entities := NewEntityStore[*fakeEntity](s.entityRepo, s.entityMarshal)
	events := NewEventStore(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarshal)
	s.store = NewStore[*fakeEntity](entities, events, s.tx)
}

func (s *StoreSuite) TearDownTest() {
	s.entityRepo.AssertExpectations(s.T())
	s.entityMarshal.AssertExpectations(s.T())
	s.eventRepo.AssertExpectations(s.T())
	s.eventMarshal.AssertExpectations(s.T())
	s.tx.AssertExpectations(s.T())
}

func (s *StoreSuite) TestSavePersistsEntityAndEventsThenClears() {
	e := newFakeEntity("1")
	evt := fakeEvent{entityID: "1", typ: "Created"}
	e.Emit(evt)

	me := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarshal.On("Marshal", mock.Anything, e).Return(me, nil)
	s.entityRepo.On("Save", mock.Anything, me).Return(nil)
	s.eventMarshal.On("Marshal", mock.Anything, evt).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := s.store.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Empty(e.events().All(), "buffer cleared after commit")
}

func (s *StoreSuite) TestSaveWithNoEventsSkipsEventStore() {
	e := newFakeEntity("1")
	me := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarshal.On("Marshal", mock.Anything, e).Return(me, nil)
	s.entityRepo.On("Save", mock.Anything, me).Return(nil)

	err := s.store.Save(s.ctx, e)

	s.Require().NoError(err)
	// eventRepo.Save / eventMarshal.Marshal not called — asserted by TearDownTest.
}

func (s *StoreSuite) TestSaveEntityFailureSkipsEventsAndKeepsBuffer() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarshal.On("Marshal", mock.Anything, e).Return(nil, errors.New("marshal boom"))

	err := s.store.Save(s.ctx, e)

	s.Require().Error(err)
	s.Len(e.events().All(), 1, "buffer intact on failure for retry")
	// eventRepo untouched — asserted by TearDownTest.
}

func (s *StoreSuite) TestSaveEventFailureKeepsBuffer() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	me := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarshal.On("Marshal", mock.Anything, e).Return(me, nil)
	s.entityRepo.On("Save", mock.Anything, me).Return(nil)
	s.eventMarshal.On("Marshal", mock.Anything, mock.Anything).Return(nil, errors.New("event boom"))

	err := s.store.Save(s.ctx, e)

	s.Require().Error(err)
	s.Len(e.events().All(), 1, "buffer intact on failure for retry")
}
