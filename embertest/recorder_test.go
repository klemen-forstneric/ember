package embertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type thingHappened struct {
	Thing string
}

func (thingHappened) Type() string       { return "thing_happened" }
func (e thingHappened) EntityID() string { return e.Thing }

type otherHappened struct{}

func (otherHappened) Type() string     { return "other_happened" }
func (otherHappened) EntityID() string { return "other" }

func TestRecorderCapturesInPublishOrder(t *testing.T) {
	r := NewRecorder(&thingHappened{}, &otherHappened{})
	ctx := context.Background()

	require.NoError(t, r.Publisher().Publish(ctx, &thingHappened{Thing: "one"}, &otherHappened{}))
	require.NoError(t, r.Publisher().Publish(ctx, &thingHappened{Thing: "two"}))

	events := r.Events()
	require.Len(t, events, 3)
	assert.Equal(t, &thingHappened{Thing: "one"}, events[0])
	assert.Equal(t, &otherHappened{}, events[1])
	assert.Equal(t, &thingHappened{Thing: "two"}, events[2])
}

func TestRecorderMintsDistinctEnvelopeIDs(t *testing.T) {
	r := NewRecorder(&thingHappened{})
	require.NoError(t, r.Publisher().Publish(context.Background(), &thingHappened{Thing: "one"}, &thingHappened{Thing: "two"}))

	require.Len(t, r.envelopes, 2)
	assert.NotEqual(t, r.envelopes[0].ID, r.envelopes[1].ID)
}

func TestRecorderReset(t *testing.T) {
	r := NewRecorder(&thingHappened{})
	require.NoError(t, r.Publisher().Publish(context.Background(), &thingHappened{Thing: "one"}))

	r.Reset()

	assert.Empty(t, r.Events())
}

// An unregistered event still reaches the sink; it only fails on decode, which
// is where the caller learns to pass it to NewRecorder.
func TestRecorderPanicsOnUnregisteredEvent(t *testing.T) {
	r := NewRecorder(&thingHappened{})
	require.NoError(t, r.Publisher().Publish(context.Background(), &otherHappened{}))

	assert.Panics(t, func() { r.Events() })
}
