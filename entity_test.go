package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// fakeEntity is a minimal Entity used as test data for store tests.
type fakeEntity struct {
	EntityRoot
	Name string
}

func newFakeEntity(id string) *fakeEntity {
	return &fakeEntity{EntityRoot: NewEntityRoot(id)}
}

func (e *fakeEntity) Type() string { return "fake" }

type EntityStoreSuite struct {
	suite.Suite
	ctx       context.Context
	repo      *mockEntityRepository
	marshaler *mockEntityMarshaler[*fakeEntity]
	store     *EntityStore[*fakeEntity]
}

func TestEntityStoreSuite(t *testing.T) {
	suite.Run(t, new(EntityStoreSuite))
}

func (s *EntityStoreSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEntityRepository{}
	s.marshaler = &mockEntityMarshaler[*fakeEntity]{}
	s.store = NewEntityStore[*fakeEntity](s.repo, s.marshaler)
}

func (s *EntityStoreSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
}

func (s *EntityStoreSuite) TestList() {
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	m2 := &MarshaledEntity{ID: "2", Type: "fake", Version: NewVersion(4), Data: []byte("bob")}
	e1 := newFakeEntity("1")
	e1.Name = "alice"
	e2 := newFakeEntity("2")
	e2.Name = "bob"

	f := Eq("name", "alice")
	// Expecting the store to forward the entity type and filter unchanged.
	s.repo.On("List", mock.Anything, "fake", f).Return([]*MarshaledEntity{m1, m2}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(e1, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m2).Return(e2, nil)

	got, err := s.store.List(s.ctx, f)

	s.Require().NoError(err)
	s.Equal([]*fakeEntity{e1, e2}, got)
}

func (s *EntityStoreSuite) TestListError() {
	sentinel := errors.New("boom")
	s.repo.On("List", mock.Anything, "fake", mock.Anything).Return(nil, sentinel)

	_, err := s.store.List(s.ctx, nil)

	s.ErrorIs(err, sentinel)
}

func (s *EntityStoreSuite) TestListUnmarshalError() {
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1), Data: []byte("alice")}
	s.repo.On("List", mock.Anything, "fake", mock.Anything).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(nil, errors.New("unmarshal boom"))

	got, err := s.store.List(s.ctx, nil)

	s.Require().Error(err)
	s.Nil(got)
}

func (s *EntityStoreSuite) TestGet() {
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	e1 := newFakeEntity("1")
	e1.Name = "alice"
	// Get derives the entity type ("fake") from the zero entity and forwards it.
	s.repo.On("Get", mock.Anything, "fake", "1").Return(m1, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(e1, nil)

	got, err := s.store.Get(s.ctx, "1")

	s.Require().NoError(err)
	s.Equal(e1, got)
}
