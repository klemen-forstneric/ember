package ember

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type EntityStoreSuite struct {
	suite.Suite
	ctx        context.Context
	repo       *mockEntityRepository
	marshaler  *mockEntityMarshaler[*fakeEntity]
	eventRepo  *mockEventRepository
	eventMarsh *mockEventMarshaler
	tx         *mockTransactor
	store      *EntityStore[*fakeEntity]
}

func TestEntityStoreSuite(t *testing.T) { suite.Run(t, new(EntityStoreSuite)) }

func (s *EntityStoreSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEntityRepository{}
	s.marshaler = &mockEntityMarshaler[*fakeEntity]{}
	s.eventRepo = &mockEventRepository{}
	s.eventMarsh = &mockEventMarshaler{}
	s.tx = &mockTransactor{}
	publisher := NewPublisher(stubIDer{id: "evt-1"}, NopMetadataGetter{}, s.eventMarsh, AtLeastOnce(s.eventRepo))
	s.store = NewEntityStore[*fakeEntity](s.repo, s.marshaler, publisher, s.tx, nil)
}

func (s *EntityStoreSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
	s.tx.AssertExpectations(s.T())
}

func (s *EntityStoreSuite) TestGetDelegatesToLoader() {
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(2)}
	e := newFakeEntity("1")
	s.repo.On("Get", mock.Anything, "fake", "1").Return(m, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m).Return(e, nil)

	got, err := s.store.Get(s.ctx, "1")

	s.Require().NoError(err)
	s.Equal(e, got)
}

func (s *EntityStoreSuite) TestListDelegatesToLoader() {
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	e1 := newFakeEntity("1")
	e1.Name = "alice"
	f := Eq("name", "alice")
	s.repo.On("List", mock.Anything, "fake", f, Sort{}).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(e1, nil)

	got, err := s.store.List(s.ctx, f, Sort{})

	s.Require().NoError(err)
	s.Equal([]*fakeEntity{e1}, got)
}

func (s *EntityStoreSuite) TestSaveDelegatesToSaver() {
	e := newFakeEntity("1")
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.marshaler.On("Marshal", mock.Anything, e).Return(m, nil)
	s.repo.On("Save", mock.Anything, m).Return(nil)
	// no events -> no tx; tx has no expectations.

	err := s.store.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Equal(uint64(1), e.Version().Value())
}
