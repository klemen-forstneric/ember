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
	sink        *mockSink
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
	s.sink = &mockSink{}
	s.tx = &mockTransactor{}
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, AtLeastOnce(s.eventRepo))
	s.saver = NewEntitySaver(publisher, s.tx, nil, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))
}

func (s *EntitySaverSuite) TearDownTest() {
	s.entityRepo.AssertExpectations(s.T())
	s.entityMarsh.AssertExpectations(s.T())
	s.eventRepo.AssertExpectations(s.T())
	s.eventMarsh.AssertExpectations(s.T())
	s.sink.AssertExpectations(s.T())
	s.tx.AssertExpectations(s.T())
}

func (s *EntitySaverSuite) TestSaveNoEntitiesIsNoop() {
	err := s.saver.Save(s.ctx)

	s.Require().NoError(err)
}

func (s *EntitySaverSuite) TestSaveSingleNoEventsSkipsTx() {
	e := newFakeEntity("1")
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)

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
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, AtLeastOnce(s.eventRepo))
	saver := NewEntitySaver(publisher, s.tx, nil,
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

func (s *EntitySaverSuite) TestSaveStampsVersionAndIndexPerEntity() {
	repo2 := &mockEntityRepository{}
	marsh2 := &mockEntityMarshaler[*fakeEntity2]{}
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, AtLeastOnce(s.eventRepo))
	saver := NewEntitySaver(publisher, s.tx, nil,
		Bind[*fakeEntity](s.entityRepo, s.entityMarsh),
		Bind[*fakeEntity2](repo2, marsh2),
	)

	e1 := newFakeEntity("1")
	e1.SetVersion(NewVersion(5))
	e1.Emit(fakeEvent{entityID: "1", typ: "A"}, fakeEvent{entityID: "1", typ: "B"})
	e2 := newFakeEntity2("2")
	e2.Emit(fakeEvent{entityID: "2", typ: "C"})

	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(6)}
	m2 := &MarshaledEntity{ID: "2", Type: "fake2", Version: NewVersion(1)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e1).Return(m1, nil)
	s.entityRepo.On("Save", mock.Anything, m1).Return(nil)
	marsh2.On("Marshal", mock.Anything, e2).Return(m2, nil)
	repo2.On("Save", mock.Anything, m2).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "T", Data: []byte(`{}`)}, nil)

	var got []EventEnvelope
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) { got = args.Get(1).([]EventEnvelope) })

	s.Require().NoError(saver.Save(s.ctx, e1, e2))

	s.Require().Len(got, 3)
	s.Equal([]uint64{6, 6, 1}, []uint64{got[0].Version, got[1].Version, got[2].Version})
	s.Equal([]int{0, 1, 0}, []int{got[0].Index, got[1].Index, got[2].Index})
	s.Equal([]string{"1", "1", "2"}, []string{got[0].EntityID, got[1].EntityID, got[2].EntityID})
	repo2.AssertExpectations(s.T())
	marsh2.AssertExpectations(s.T())
}

func (s *EntitySaverSuite) TestSaveForeignEventErrors() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "2", typ: "A"})
	version := e.Version()

	err := s.saver.Save(s.ctx, e)

	s.Require().ErrorIs(err, ErrForeignEvent)
	s.Equal(version, e.Version())
	s.Len(e.events().All(), 1)
	s.entityRepo.AssertNotCalled(s.T(), "Save", mock.Anything, mock.Anything)
	s.eventRepo.AssertNotCalled(s.T(), "Save", mock.Anything, mock.Anything)
}

// bestEffortSaver builds an EntitySaver on a BestEffort publisher against the
// suite's sink and a fresh recordingTransactor. l may be nil (defaults to NopLogger).
func (s *EntitySaverSuite) bestEffortSaver(l LoggerCtx) (*EntitySaver, *recordingTransactor) {
	tx := &recordingTransactor{}
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, BestEffort(s.sink))
	return NewEntitySaver(publisher, tx, l, Bind[*fakeEntity](s.entityRepo, s.entityMarsh)), tx
}

func (s *EntitySaverSuite) TestBestEffortDeliversAfterCommit() {
	saver, tx := s.bestEffortSaver(nil)

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(mock.Arguments) {
			s.True(tx.committed, "delivery must run after commit")
		})

	err := saver.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Empty(e.events().All())
	s.Equal(uint64(1), e.Version().Value())
}

func (s *EntitySaverSuite) TestBestEffortCommitFailureDoesNotDeliver() {
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, BestEffort(s.sink))
	saver := NewEntitySaver(publisher, s.tx, nil, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	version := e.Version()
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	commitErr := errors.New("commit boom")
	s.tx.On("WithinTx", mock.Anything).Return(commitErr).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)

	err := saver.Save(s.ctx, e)

	s.Require().ErrorIs(err, commitErr)
	s.Require().NotErrorIs(err, ErrDeliveryFailed)
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)
	s.Equal(version, e.Version())
	s.Len(e.events().All(), 1)
}

func (s *EntitySaverSuite) TestBestEffortDeliveryIgnoresPostCommitCancellation() {
	ctx, cancel := context.WithCancel(context.Background())
	tx := &cancelAfterCommitTransactor{cancel: cancel}
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, BestEffort(s.sink))
	saver := NewEntitySaver(publisher, tx, nil, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)

	var deliveredCtx context.Context
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) {
			deliveredCtx = args.Get(0).(context.Context)
		})

	err := saver.Save(ctx, e)

	s.Require().NoError(err)
	s.Require().NotNil(deliveredCtx)
	s.NoError(deliveredCtx.Err())
}

func (s *EntitySaverSuite) TestBestEffortDeliveryFailureStillAdvancesEntity() {
	saver, _ := s.bestEffortSaver(nil)

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(errors.New("broker down")).Once()

	err := saver.Save(s.ctx, e)

	s.Require().ErrorIs(err, ErrDeliveryFailed)
	// State committed, so the entity must match durable state regardless.
	s.Equal(uint64(1), e.Version().Value())
	s.Empty(e.events().All())
}

const joinedTxWarning = "Delivering events inside a caller-owned transaction; a rollback will publish events for uncommitted state"

func (s *EntitySaverSuite) TestBestEffortWarnsWhenJoiningCallerTx() {
	logger := &mockLogger{}
	saver, tx := s.bestEffortSaver(logger)
	tx.inTx = true

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()
	logger.On("Warn", joinedTxWarning).Return().Once()

	err := saver.Save(s.ctx, e)

	s.Require().NoError(err)
	logger.AssertExpectations(s.T())
}

func (s *EntitySaverSuite) TestBestEffortSilentWhenEmberOwnsTx() {
	logger := &mockLogger{}
	saver, tx := s.bestEffortSaver(logger)
	tx.inTx = false

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()

	err := saver.Save(s.ctx, e)

	s.Require().NoError(err)
	logger.AssertNotCalled(s.T(), "Warn", mock.Anything)
}

func (s *EntitySaverSuite) TestSaveJoinedTxAtLeastOnceDoesNotWarn() {
	s.tx.inTx = true
	logger := &mockLogger{}
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, AtLeastOnce(s.eventRepo))
	saver := NewEntitySaver(publisher, s.tx, logger, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := saver.Save(s.ctx, e)

	s.Require().NoError(err)
	logger.AssertNotCalled(s.T(), "Warn", mock.Anything)
}
