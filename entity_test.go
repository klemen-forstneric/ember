package ember

import (
	"testing"

	"github.com/stretchr/testify/require"
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

// fakeEntity2 is a second entity type, for cross-type saver tests.
type fakeEntity2 struct {
	EntityRoot
	Name string
}

func newFakeEntity2(id string) *fakeEntity2 { return &fakeEntity2{EntityRoot: NewEntityRoot(id)} }
func (e *fakeEntity2) Type() string         { return "fake2" }

func TestEntityRootEmitBuffersEvents(t *testing.T) {
	e := newFakeEntity("1")
	evt := fakeEvent{entityID: "1", typ: "Created"}

	e.Emit(evt)

	require.Equal(t, []Event{evt}, e.events().All())
}

func TestEntityRootIdentityUnchanged(t *testing.T) {
	e := newFakeEntity("42")
	require.Equal(t, "42", e.ID())
	require.Equal(t, uint64(0), e.Version().Value())

	e.SetVersion(NewVersion(3))
	require.Equal(t, uint64(3), e.Version().Value())
}
