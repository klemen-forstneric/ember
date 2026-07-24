package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type EntitySaverSuite struct {
	suite.Suite
	ctx         context.Context
	entityRepo  *mockEntityRepository
	entityMarsh *mockEntityMarshaler[*fakeEntity]
	eventRepo   *mockEventRepository
	eventMarsh  *mockEventMarshaler
	tx          *mockTransactor
	saver       *EntitySaver
}

func TestEntitySaverSuite(t *testing.T) { suite.Run(t, new(EntitySaverSuite)) }

func (s *EntitySaverSuite) SetupTest() {
	s.ctx = context.Background()
	s.entityRepo = &mockEntityRepository{}
	s.entityMarsh = &mockEntityMarshaler[*fakeEntity]{}
	s.eventRepo = &mockEventRepository{}
	s.eventMarsh = &mockEventMarshaler{}
	s.tx = &mockTransactor{}
	events := NewEventStore(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarsh)
	s.saver = NewEntitySaver(events, s.tx, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))
}

func (s *EntitySaverSuite) TearDownTest() {
	s.entityRepo.AssertExpectations(s.T())
	s.entityMarsh.AssertExpectations(s.T())
	s.eventRepo.AssertExpectations(s.T())
	s.eventMarsh.AssertExpectations(s.T())
	s.tx.AssertExpectations(s.T())
}

func (s *EntitySaverSuite) TestSaveNoEntitiesIsNoop() {
	// no expectations set anywhere; TearDownTest catches any unexpected call.
	err := s.saver.Save(s.ctx)

	s.Require().NoError(err)
}

func (s *EntitySaverSuite) TestSaveSingleNoEventsSkipsTx() {
	e := newFakeEntity("1")
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	// tx.WithinTx must NOT be called (no expectations set) — asserted by TearDownTest.

	err := s.saver.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Equal(uint64(1), e.Version().Value()) // version advanced post-write
}

func (s *EntitySaverSuite) TestSaveSingleWithEventsUsesTxAndClears() {
	e := newFakeEntity("1")
	evt := fakeEvent{entityID: "1", typ: "Created"}
	e.Emit(evt)
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, evt).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := s.saver.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Empty(e.events().All())
	s.Equal(uint64(1), e.Version().Value())
}

func (s *EntitySaverSuite) TestSaveUnregisteredType() {
	err := s.saver.Save(s.ctx, newFakeEntity2("1")) // fake2 not bound

	s.Require().ErrorIs(err, ErrUnregisteredEntity)
}

func (s *EntitySaverSuite) TestSaveEntityFailureLeavesEntityUntouched() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	version := e.Version()
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(nil, errors.New("boom"))

	err := s.saver.Save(s.ctx, e)

	s.Require().Error(err)
	s.Equal(version, e.Version()) // no bump leaked
	s.Len(e.events().All(), 1)    // buffer intact for retry
}

func (s *EntitySaverSuite) TestSaveEventFailureLeavesEntityUntouched() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	version := e.Version()
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).Return(nil, errors.New("event boom"))

	err := s.saver.Save(s.ctx, e)

	s.Require().Error(err)
	s.Equal(version, e.Version())
	s.Len(e.events().All(), 1)
}

func (s *EntitySaverSuite) TestSaveCommitErrorLeavesEntityUntouched() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	version := e.Version()
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	commitErr := errors.New("commit boom")
	s.tx.On("WithinTx", mock.Anything).Return(commitErr).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := s.saver.Save(s.ctx, e)

	s.Require().ErrorIs(err, commitErr)
	s.Equal(version, e.Version())
	s.Len(e.events().All(), 1)
}

func (s *EntitySaverSuite) TestSaveTwoTypesOneTx() {
	repo2 := &mockEntityRepository{}
	marsh2 := &mockEntityMarshaler[*fakeEntity2]{}
	events := NewEventStore(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarsh)
	saver := NewEntitySaver(events, s.tx,
		Bind[*fakeEntity](s.entityRepo, s.entityMarsh),
		Bind[*fakeEntity2](repo2, marsh2),
	)
	e1 := newFakeEntity("1")
	e2 := newFakeEntity2("2")
	e1.Emit(fakeEvent{entityID: "1", typ: "A"})
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	m2 := &MarshaledEntity{ID: "2", Type: "fake2", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "A", Data: []byte(`{}`)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e1).Return(m1, nil)
	s.entityRepo.On("Save", mock.Anything, m1).Return(nil)
	marsh2.On("Marshal", mock.Anything, e2).Return(m2, nil)
	repo2.On("Save", mock.Anything, m2).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := saver.Save(s.ctx, e1, e2)

	s.Require().NoError(err)
	s.Empty(e1.events().All())
	s.Equal(uint64(1), e1.Version().Value())
	s.Equal(uint64(1), e2.Version().Value())
	repo2.AssertExpectations(s.T())
	marsh2.AssertExpectations(s.T())
}
