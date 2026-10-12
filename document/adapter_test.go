package document

import (
	"context"
	"errors"
	. "github.com/klemen-forstneric/ember"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
	s.loader = NewEntityLoader(Bind(NewAdapter(s.repo, s.marshaler)))
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
	s.repo.On("List", mock.Anything, "fake", f, Sort{}, Unpaged()).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(e1, nil)

	got, err := s.loader.List(s.ctx, f, Sort{})

	s.Require().NoError(err)
	s.Equal([]*fakeEntity{e1}, got)
}

func (s *EntityLoaderSuite) TestListError() {
	sentinel := errors.New("boom")
	s.repo.On("List", mock.Anything, "fake", mock.Anything, mock.Anything, mock.Anything).Return(nil, sentinel)

	_, err := s.loader.List(s.ctx, nil, Sort{})

	s.ErrorIs(err, sentinel)
}

func (s *EntityLoaderSuite) TestListUnmarshalError() {
	m1 := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}
	s.repo.On("List", mock.Anything, "fake", mock.Anything, mock.Anything, mock.Anything).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(nil, errors.New("unmarshal boom"))

	got, err := s.loader.List(s.ctx, nil, Sort{})

	s.Require().Error(err)
	s.Nil(got)
}

func (s *EntityLoaderSuite) TestListPagePassesPageThrough() {
	f := Eq("k", "v")
	p := Limit(2).After(int64(7), "1")
	m1 := &MarshaledEntity{ID: "2", Type: "fake"}
	s.repo.On("List", mock.Anything, "fake", f, AscNum("seq"), p).Return([]*MarshaledEntity{m1}, nil)
	s.marshaler.On("Unmarshal", mock.Anything, m1).Return(newFakeEntity("2"), nil)

	got, err := s.loader.ListPage(s.ctx, f, AscNum("seq"), p)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
}

func (s *EntityLoaderSuite) TestListPageRejectsInvalidPage() {
	_, err := s.loader.ListPage(s.ctx, nil, Unsorted(), Page{Offset: 5})

	s.Require().ErrorIs(err, ErrInvalidPage)
	s.repo.AssertNotCalled(s.T(), "List")
}

func TestSaveRestoresVersionOnFailureAndPanic(t *testing.T) {
	for _, mode := range []string{"success", "marshal error", "write error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			r := &mockEntityRepository{}
			m := &mockEntityMarshaler[*fakeEntity]{}
			a := NewAdapter(r, m)
			e := newFakeEntity("one")
			e.SetVersion(NewVersion(7))
			next := e.Version().Inc()
			payload := &MarshaledEntity{Data: []byte("snapshot")}
			failure := errors.New("failed")
			call := m.On("Marshal", mock.Anything, e).Run(func(mock.Arguments) {
				require.Equal(t, next, e.Version())
			})
			switch mode {
			case "panic":
				call.Panic("bad codec")
			case "marshal error":
				call.Return(nil, failure)
			default:
				call.Return(payload, nil)
				var writeErr error
				if mode == "write error" {
					writeErr = failure
				}
				r.On("Save", mock.Anything, payload).Return(writeErr).Run(func(mock.Arguments) {
					require.Equal(t, NewVersion(7), e.Version())
					require.Equal(t, "one", payload.ID)
					require.Equal(t, "fake", payload.Type)
					require.Equal(t, next, payload.Version)
				})
			}
			if mode == "panic" {
				require.Panics(t, func() { _ = a.Save(context.Background(), e, next) })
			} else {
				err := a.Save(context.Background(), e, next)
				if mode == "success" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, failure)
				}
			}
			require.Equal(t, NewVersion(7), e.Version())
			m.AssertExpectations(t)
			r.AssertExpectations(t)
		})
	}
}
