package wal

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

// The relay must reconstruct exactly what the repository wrote, metadata
// included: pulsar.Publisher fails a publish with no correlation id.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	ts := time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC)
	in := ember.EventEnvelope{
		ID:        "e1",
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("correlation_id"): "c-1"},
		Timestamp: ts,
	}

	payload, err := encode(in)
	require.NoError(t, err)

	out, err := decode(payload)
	require.NoError(t, err)

	require.Equal(t, in.ID, out.ID)
	require.Equal(t, in.EntityID, out.EntityID)
	require.Equal(t, in.Event.Type, out.Event.Type)
	require.JSONEq(t, string(in.Event.Data), string(out.Event.Data))
	require.Equal(t, "c-1", out.Metadata[ember.MetadataKey("correlation_id")])
	require.True(t, in.Timestamp.Equal(out.Timestamp))
}

func TestEncodeDecodeRoundTripsTheOrderingKey(t *testing.T) {
	e := ember.EventEnvelope{
		ID:        "e1",
		EntityID:  "A",
		Version:   7,
		Index:     2,
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("corr"): "c-1"},
		Timestamp: time.Unix(1_700_000_000, 0).UTC(),
	}

	b, err := encode(e)
	require.NoError(t, err)
	got, err := decode(b)

	require.NoError(t, err)
	require.Equal(t, uint64(7), got.Version)
	require.Equal(t, 2, got.Index)
	require.Equal(t, e.EntityID, got.EntityID)
	require.Equal(t, e.Event.Type, got.Event.Type)
}

func TestEncodeUsesTheOutboxWireKeys(t *testing.T) {
	b, err := encode(ember.EventEnvelope{
		ID:        "e1",
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("correlation_id"): "c-1"},
		Version:   7,
		Index:     2,
		Timestamp: time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &raw))
	require.ElementsMatch(t,
		[]string{"id", "entity_id", "type", "data", "metadata", "version", "idx", "created_at"},
		slices.Collect(maps.Keys(raw)))
}

func TestDecodeRejectsMalformedPayload(t *testing.T) {
	_, err := decode([]byte(`not json`))
	require.Error(t, err)
}
