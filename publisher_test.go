package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type PublisherSuite struct {
	suite.Suite
	ctx       context.Context
	repo      *mockEventRepository
	sink      *mockSink
	marshaler *mockEventMarshaler
}

func TestPublisherSuite(t *testing.T) { suite.Run(t, new(PublisherSuite)) }

func (s *PublisherSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEventRepository{}
	s.sink = &mockSink{}
	s.marshaler = &mockEventMarshaler{}
}

func (s *PublisherSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.sink.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
}

func (s *PublisherSuite) atLeastOncePublisher() *Publisher {
	return NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.marshaler, AtLeastOnce(s.repo))
}

func (s *PublisherSuite) bestEffortPublisher() *Publisher {
	return NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.marshaler, BestEffort(s.sink))
}

func (s *PublisherSuite) TestAtLeastOncePersistsEnvelopes() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	marshaled := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(marshaled, nil)
	s.repo.On("Save", mock.Anything, mock.MatchedBy(func(envs []EventEnvelope) bool {
		return len(envs) == 1 &&
			envs[0].ID == "evt-1" &&
			envs[0].EntityID == "A" &&
			envs[0].Event == marshaled
	})).Return(nil)

	err := s.atLeastOncePublisher().Publish(s.ctx, evt)

	s.Require().NoError(err)
}

func (s *PublisherSuite) TestAtLeastOncePreservesEmitOrder() {
	evt1 := fakeEvent{entityID: "A", typ: "Created"}
	evt2 := fakeEvent{entityID: "B", typ: "Updated"}
	m1 := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	m2 := &MarshaledEvent{Type: "Updated", Data: []byte(`{}`)}
	s.marshaler.On("Marshal", mock.Anything, evt1).Return(m1, nil)
	s.marshaler.On("Marshal", mock.Anything, evt2).Return(m2, nil)
	s.repo.On("Save", mock.Anything, mock.MatchedBy(func(envs []EventEnvelope) bool {
		return len(envs) == 2 &&
			envs[0].EntityID == "A" && envs[0].Event == m1 &&
			envs[1].EntityID == "B" && envs[1].Event == m2
	})).Return(nil).Once()

	err := s.atLeastOncePublisher().Publish(s.ctx, evt1, evt2)

	s.Require().NoError(err)
}

func (s *PublisherSuite) TestAtLeastOnceMarshalError() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(nil, errors.New("boom"))

	err := s.atLeastOncePublisher().Publish(s.ctx, evt)

	s.Require().Error(err)
}

func (s *PublisherSuite) TestAtLeastOnceRepositoryError() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	repoErr := errors.New("outbox down")
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)
	s.repo.On("Save", mock.Anything, mock.Anything).Return(repoErr)

	err := s.atLeastOncePublisher().Publish(s.ctx, evt)

	s.Require().ErrorIs(err, repoErr)
}

func (s *PublisherSuite) TestAtLeastOnceStageDefersNothing() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil)

	d, err := s.atLeastOncePublisher().stage(s.ctx, []staged{{event: evt, version: 1}})

	s.Require().NoError(err)
	s.Nil(d, "the Relay delivers; nothing waits on commit")
}

func (s *PublisherSuite) TestBestEffortPublishesToSink() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	marshaled := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(marshaled, nil)
	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(envs []EventEnvelope) bool {
		return len(envs) == 1 && envs[0].ID == "evt-1" && envs[0].Event == marshaled
	})).Return(nil)

	err := s.bestEffortPublisher().Publish(s.ctx, evt)

	s.Require().NoError(err)
}

func (s *PublisherSuite) TestBestEffortSinkError() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	sinkErr := errors.New("broker down")
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(sinkErr)

	err := s.bestEffortPublisher().Publish(s.ctx, evt)

	s.Require().ErrorIs(err, sinkErr)
}

func (s *PublisherSuite) TestBestEffortStageDefersDelivery() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)

	d, err := s.bestEffortPublisher().stage(s.ctx, []staged{{event: evt, version: 1}})

	s.Require().NoError(err)
	s.Require().NotNil(d, "delivery must wait for commit")
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)

	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()
	s.Require().NoError(d(s.ctx))
}

func (s *PublisherSuite) splitPublisher() *Publisher {
	return NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.marshaler, Split(AtLeastOnce(s.repo), BestEffort(s.sink), fakeEvent{typ: "Frame"}))
}

// marshals makes each event marshal to its own type.
func (s *PublisherSuite) marshals(events ...fakeEvent) {
	for _, e := range events {
		s.marshaler.On("Marshal", mock.Anything, e).Return(&MarshaledEvent{Type: e.typ, Data: []byte(`{}`)}, nil)
	}
}

// indexes returns the emit indexes of envelopes, in the order given.
func indexes(envs []EventEnvelope) []int {
	var out []int
	for _, e := range envs {
		out = append(out, e.Index)
	}
	return out
}

func (s *PublisherSuite) TestSplitStagesEachSideWithItsGuaranteeInEmitOrder() {
	created := fakeEvent{entityID: "A", typ: "Created"}
	frame := fakeEvent{entityID: "A", typ: "Frame"}
	updated := fakeEvent{entityID: "A", typ: "Updated"}
	s.marshals(created, frame, updated)
	var saved, published []EventEnvelope
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) { saved = args.Get(1).([]EventEnvelope) })

	d, err := s.splitPublisher().stage(s.ctx, []staged{
		{event: created, version: 1, index: 0},
		{event: frame, version: 1, index: 1},
		{event: updated, version: 1, index: 2},
	})

	s.Require().NoError(err)
	s.Equal([]int{0, 2}, indexes(saved), "the rest goes to the outbox while staging")
	s.Require().NotNil(d, "the frame waits for commit")
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)

	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) { published = args.Get(1).([]EventEnvelope) })
	s.Require().NoError(d(s.ctx))
	s.Equal([]int{1}, indexes(published))
}

func (s *PublisherSuite) TestSplitWithoutListedEventsDefersNothing() {
	created := fakeEvent{entityID: "A", typ: "Created"}
	s.marshals(created)
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil).Once()

	d, err := s.splitPublisher().stage(s.ctx, []staged{{event: created, version: 1}})

	s.Require().NoError(err)
	s.Nil(d)
}

func (s *PublisherSuite) TestSplitWithOnlyListedEventsSkipsTheOutbox() {
	frame := fakeEvent{entityID: "A", typ: "Frame"}
	s.marshals(frame)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()

	s.Require().NoError(s.splitPublisher().Publish(s.ctx, frame))

	s.repo.AssertNotCalled(s.T(), "Save", mock.Anything, mock.Anything)
}

func (s *PublisherSuite) TestSplitOutboxErrorStagesNoDelivery() {
	created := fakeEvent{entityID: "A", typ: "Created"}
	frame := fakeEvent{entityID: "A", typ: "Frame"}
	s.marshals(created, frame)
	repoErr := errors.New("outbox down")
	s.repo.On("Save", mock.Anything, mock.Anything).Return(repoErr)

	d, err := s.splitPublisher().stage(s.ctx, []staged{{event: created, version: 1}, {event: frame, version: 1, index: 1}})

	s.Require().ErrorIs(err, repoErr)
	s.Nil(d)
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)
}

func (s *PublisherSuite) TestPublishStampsUnorderedLane() {
	evt1 := fakeEvent{entityID: "A", typ: "Created"}
	evt2 := fakeEvent{entityID: "A", typ: "Updated"}
	s.marshaler.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "T", Data: []byte(`{}`)}, nil)

	var got []EventEnvelope
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) { got = args.Get(1).([]EventEnvelope) })

	s.Require().NoError(s.atLeastOncePublisher().Publish(s.ctx, evt1, evt2))

	s.Require().Len(got, 2)
	s.Equal([]uint64{0, 0}, []uint64{got[0].Version, got[1].Version})
	s.Equal([]int{0, 1}, []int{got[0].Index, got[1].Index})
}

func (s *PublisherSuite) TestOrderingKeyComesFromTheEvent() {
	evt := orderedEvent{
		entityID:    "message-1",
		orderingKey: "chat-1",
	}
	s.marshaler.On("Marshal", mock.Anything, evt).
		Return(&MarshaledEvent{Type: "Ordered", Data: []byte(`{}`)}, nil)

	var got []EventEnvelope
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) { got = args.Get(1).([]EventEnvelope) })

	s.Require().NoError(s.atLeastOncePublisher().Publish(s.ctx, evt))

	s.Require().Len(got, 1)
	s.Equal("message-1", got[0].EntityID)
	s.Equal("chat-1", got[0].OrderingKey)
}

func (s *PublisherSuite) TestOrderingKeyDefaultsToTheEntityID() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)

	var got []EventEnvelope
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(args mock.Arguments) { got = args.Get(1).([]EventEnvelope) })

	s.Require().NoError(s.atLeastOncePublisher().Publish(s.ctx, evt))

	s.Require().Len(got, 1)
	s.Equal("A", got[0].OrderingKey)
}

func (s *PublisherSuite) TestPublishNoEventsIsNoop() {
	s.Require().NoError(s.atLeastOncePublisher().Publish(s.ctx))
	s.Require().NoError(s.bestEffortPublisher().Publish(s.ctx))
}
