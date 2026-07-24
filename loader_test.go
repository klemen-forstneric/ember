package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type EntityLoaderSuite struct {
	suite.Suite
	ctx       context.Context
	repo      *mockEntityRepository
	marshaler *mockEntityMarshaler[*fakeEntity]
	loader    *EntityLoader[*fakeEntity]
}

func TestEntityLoaderSuite(t *testing.T) { suite.Run(t, new(EntityLoaderSuite)) }

func (s *EntityLoaderSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEntityRepository{}
	s.marshaler = &mockEntityMarshaler[*fakeEntity]{}
	s.loader = NewEntityLoader(Bind[*fakeEntity](s.repo, s.marshaler))
}

func (s *EntityLoaderSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
}

func (s *EntityLoaderSuite) TestGet() {
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	e := newFakeEntity("1")
	e.Name = "alice"
	s.repo.On("Get", mock.Anything, "fake", "1").Return(m, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m).Return(e, nil)

	got, err := s.loader.Get(s.ctx, "1")

	s.Require().NoError(err)
	s.Equal(e, got)
}

func (s *EntityLoaderSuite) TestList() {
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	e1 := newFakeEntity("1")
	e1.Name = "alice"
	f := Eq("name", "alice")
	s.repo.On("List", mock.Anything, "fake", f, Sort{}).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(e1, nil)

	got, err := s.loader.List(s.ctx, f, Sort{})

	s.Require().NoError(err)
	s.Equal([]*fakeEntity{e1}, got)
}

func (s *EntityLoaderSuite) TestListError() {
	sentinel := errors.New("boom")
	s.repo.On("List", mock.Anything, "fake", mock.Anything, mock.Anything).Return(nil, sentinel)

	_, err := s.loader.List(s.ctx, nil, Sort{})

	s.ErrorIs(err, sentinel)
}

func (s *EntityLoaderSuite) TestListUnmarshalError() {
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	s.repo.On("List", mock.Anything, "fake", mock.Anything, mock.Anything).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(nil, errors.New("unmarshal boom"))

	got, err := s.loader.List(s.ctx, nil, Sort{})

	s.Require().Error(err)
	s.Nil(got)
}
