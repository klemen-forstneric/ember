package ember

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeEvent is minimal test data implementing Event.
type fakeEvent struct {
	entityID string
	typ      string
}

func (e fakeEvent) EntityID() string { return e.entityID }
func (e fakeEvent) Type() string     { return e.typ }

func TestEventsEmitAndAll(t *testing.T) {
	var buf Events
	a := fakeEvent{entityID: "1", typ: "A"}
	b := fakeEvent{entityID: "1", typ: "B"}

	buf.Emit(a)
	buf.Emit(b)

	require.Equal(t, []Event{a, b}, buf.All())
}

func TestEventsAllClonesBuffer(t *testing.T) {
	var buf Events
	buf.Emit(fakeEvent{entityID: "1", typ: "A"})

	got := buf.All()
	got[0] = fakeEvent{entityID: "x", typ: "X"} // mutate the returned slice

	require.Equal(t, "A", buf.All()[0].Type()) // buffer unaffected
}

func TestEventsClearThenEmit(t *testing.T) {
	var buf Events
	buf.Emit(fakeEvent{entityID: "1", typ: "A"})
	buf.Clear()
	require.Empty(t, buf.All())

	buf.Emit(fakeEvent{entityID: "1", typ: "B"}) // append after nil must not panic
	require.Len(t, buf.All(), 1)
	require.Equal(t, "B", buf.All()[0].Type())
}
