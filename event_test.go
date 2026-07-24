package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type EventStoreSuite struct {
	suite.Suite
	ctx       context.Context
	repo      *mockEventRepository
	marshaler *mockEventMarshaler
	store     *EventStore
}

func TestEventStoreSuite(t *testing.T) {
	suite.Run(t, new(EventStoreSuite))
}

func (s *EventStoreSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEventRepository{}
	s.marshaler = &mockEventMarshaler{}
	s.store = NewEventStore(stubIDer{id: "evt-1"}, s.repo, NoopMetadataGetter{}, s.marshaler)
}

func (s *EventStoreSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
}

func (s *EventStoreSuite) TestSavePersistsEnvelopes() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	marshaled := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(marshaled, nil)
	s.repo.On("Save", mock.Anything, mock.MatchedBy(func(envs []EventEnvelope) bool {
		return len(envs) == 1 &&
			envs[0].ID == "evt-1" &&
			envs[0].EntityID == "A" &&
			envs[0].Event == marshaled
	})).Return(nil)

	err := s.store.Save(s.ctx, evt)

	s.Require().NoError(err)
}

func (s *EventStoreSuite) TestSaveMarshalError() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(nil, errors.New("boom"))

	err := s.store.Save(s.ctx, evt)

	s.Require().Error(err)
	// repo.Save must NOT be called — asserted by TearDownTest.
}
