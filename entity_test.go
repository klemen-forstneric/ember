package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// versionedRepo is an in-memory EntityRepository that enforces optimistic
// concurrency the way the persistent backends do: a save whose expected
// (initial) version does not match the stored version is rejected. It exists to
// exercise repeated saves of a single in-memory entity.
type versionedRepo struct{ docs map[string]*MarshaledEntity }

func newVersionedRepo() *versionedRepo { return &versionedRepo{docs: map[string]*MarshaledEntity{}} }

func (r *versionedRepo) Save(_ context.Context, m *MarshaledEntity) error {
	key := m.Type + "/" + m.ID
	if cur, ok := r.docs[key]; ok {
		if cur.Version.Value() != m.Version.Initial() {
			return ErrVersionConflict
		}
	} else if m.Version.Initial() != 0 {
		return ErrVersionConflict
	}
	stored := *m
	r.docs[key] = &stored
	return nil
}

func (r *versionedRepo) Get(_ context.Context, typ, id string) (*MarshaledEntity, error) {
	if m, ok := r.docs[typ+"/"+id]; ok {
		return m, nil
	}
	return nil, ErrEntityNotFound
}

func (r *versionedRepo) List(context.Context, string, Filter) ([]*MarshaledEntity, error) {
	return nil, nil
}

// versionMarshaler mirrors the version handling of the real JSON marshaler
// without pulling in the ember/json package (which would import ember).
type versionMarshaler struct{}

func (versionMarshaler) Marshal(_ context.Context, e *fakeEntity) (*MarshaledEntity, error) {
	return &MarshaledEntity{ID: e.ID(), Type: e.Type(), Version: e.Version()}, nil
}

func (versionMarshaler) Unmarshal(_ context.Context, m *MarshaledEntity) (*fakeEntity, error) {
	e := newFakeEntity(m.ID)
	e.SetVersion(m.Version)
	return e, nil
}

// A single in-memory entity saved repeatedly (create, then updates) must not
// self-conflict: after each save the store collapses the version so the next
// save's optimistic filter matches the just-persisted version.
func TestEntityStoreRepeatedSaveOfSameInstance(t *testing.T) {
	store := NewEntityStore[*fakeEntity](newVersionedRepo(), versionMarshaler{})
	ctx := context.Background()
	e := newFakeEntity("1")

	require.NoError(t, store.Save(ctx, e)) // create -> stored version 1
	e.Name = "second"
	require.NoError(t, store.Save(ctx, e)) // stored version 2
	e.Name = "third"
	require.NoError(t, store.Save(ctx, e)) // stored version 3
}

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
