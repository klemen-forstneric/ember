package ember

import (
	"context"
	"errors"
	"testing"
)

// fakeEntity is a minimal Entity for store tests.
type fakeEntity struct {
	EntityRoot
	Name string
}

func newFakeEntity(id string) *fakeEntity {
	return &fakeEntity{EntityRoot: NewEntityRoot(id)}
}

func (e *fakeEntity) Type() string { return "fake" }

// fakeMarshaler hydrates fakeEntity from a MarshaledEntity (ID + Version only).
type fakeMarshaler struct{}

func (fakeMarshaler) Marshal(_ context.Context, e *fakeEntity) (*MarshaledEntity, error) {
	return &MarshaledEntity{ID: e.ID(), Type: e.Type(), Version: e.Version(), Data: []byte(e.Name)}, nil
}

func (fakeMarshaler) Unmarshal(_ context.Context, m *MarshaledEntity) (*fakeEntity, error) {
	e := newFakeEntity(m.ID)
	e.Name = string(m.Data)
	e.SetVersion(m.Version)
	return e, nil
}

// fakeRepo records calls and returns canned results.
type fakeRepo struct {
	getResult  *MarshaledEntity
	getErr     error
	listResult []*MarshaledEntity
	listErr    error
	gotType    string
	gotFilter  Filter
}

func (r *fakeRepo) Save(_ context.Context, _ *MarshaledEntity) error { return nil }

func (r *fakeRepo) Get(_ context.Context, typ, _ string) (*MarshaledEntity, error) {
	r.gotType = typ
	return r.getResult, r.getErr
}

func (r *fakeRepo) List(_ context.Context, typ string, f Filter) ([]*MarshaledEntity, error) {
	r.gotType = typ
	r.gotFilter = f
	return r.listResult, r.listErr
}

func TestEntityStoreList(t *testing.T) {
	repo := &fakeRepo{listResult: []*MarshaledEntity{
		{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")},
		{ID: "2", Type: "fake", Version: NewVersion(4), Data: []byte("bob")},
	}}
	store := NewEntityStore[*fakeEntity](repo, fakeMarshaler{})

	f := Eq("name", "alice")
	got, err := store.List(context.Background(), f)
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if repo.gotType != "fake" {
		t.Errorf("repo received type %q, want %q", repo.gotType, "fake")
	}
	if repo.gotFilter != f {
		t.Errorf("repo received filter %#v, want %#v", repo.gotFilter, f)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entities, want 2", len(got))
	}
	if got[0].ID() != "1" || got[0].Name != "alice" {
		t.Errorf("entity[0] = %+v, want id=1 name=alice", got[0])
	}
}

func TestEntityStoreListError(t *testing.T) {
	sentinel := errors.New("boom")
	repo := &fakeRepo{listErr: sentinel}
	store := NewEntityStore[*fakeEntity](repo, fakeMarshaler{})

	_, err := store.List(context.Background(), nil)
	if !errors.Is(err, sentinel) {
		t.Errorf("got error %v, want %v", err, sentinel)
	}
}

func TestEntityStoreGet(t *testing.T) {
	repo := &fakeRepo{getResult: &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(3), Data: []byte("alice")}}
	store := NewEntityStore[*fakeEntity](repo, fakeMarshaler{})

	got, err := store.Get(context.Background(), "1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.ID() != "1" || got.Name != "alice" {
		t.Errorf("entity = %+v, want id=1 name=alice", got)
	}
}
