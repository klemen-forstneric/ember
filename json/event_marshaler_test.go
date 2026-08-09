package json

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

type pointerEvent struct {
	Entity string `json:"entity"`
	N      int    `json:"n"`
}

func (e *pointerEvent) EntityID() string { return e.Entity }
func (e *pointerEvent) Type() string     { return "pointer" }

type valueEvent struct {
	Entity string `json:"entity"`
	N      int    `json:"n"`
}

func (e valueEvent) EntityID() string { return e.Entity }
func (e valueEvent) Type() string     { return "value" }

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	m := NewEventMarshaler(&pointerEvent{})
	ctx := context.Background()

	marshaled, err := m.Marshal(ctx, &pointerEvent{Entity: "a", N: 7})
	require.NoError(t, err)
	require.Equal(t, "pointer", marshaled.Type)

	got, err := m.Unmarshal(ctx, marshaled)
	require.NoError(t, err)
	require.Equal(t, &pointerEvent{Entity: "a", N: 7}, got)
}

func TestRegistrationAcceptsValueAndPointerPrototypes(t *testing.T) {
	m := NewEventMarshaler(&pointerEvent{}, valueEvent{})
	ctx := context.Background()

	p, err := m.Unmarshal(ctx, &ember.MarshaledEvent{Type: "pointer", Data: []byte(`{"entity":"a","n":1}`)})
	require.NoError(t, err)
	require.Equal(t, &pointerEvent{Entity: "a", N: 1}, p)

	v, err := m.Unmarshal(ctx, &ember.MarshaledEvent{Type: "value", Data: []byte(`{"entity":"b","n":2}`)})
	require.NoError(t, err)
	require.Equal(t, &valueEvent{Entity: "b", N: 2}, v)
}

func TestUnmarshalUnknownType(t *testing.T) {
	m := NewEventMarshaler(&pointerEvent{})

	_, err := m.Unmarshal(context.Background(), &ember.MarshaledEvent{Type: "nope", Data: []byte(`{}`)})

	require.ErrorIs(t, err, ember.ErrUnknownEvent)
}

func TestUnmarshalMalformedPayload(t *testing.T) {
	m := NewEventMarshaler(&pointerEvent{})

	_, err := m.Unmarshal(context.Background(), &ember.MarshaledEvent{Type: "pointer", Data: []byte(`not json`)})

	require.Error(t, err)
}

func TestNewEventMarshalerSkipsNilPrototype(t *testing.T) {
	var nilEvent ember.Event

	require.NotPanics(t, func() { NewEventMarshaler(nilEvent, &pointerEvent{}) })

	m := NewEventMarshaler(nilEvent, &pointerEvent{})
	got, err := m.Unmarshal(context.Background(),
		&ember.MarshaledEvent{Type: "pointer", Data: []byte(`{"entity":"a","n":1}`)})
	require.NoError(t, err)
	require.Equal(t, &pointerEvent{Entity: "a", N: 1}, got)
}
