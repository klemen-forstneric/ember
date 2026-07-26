package wal

import (
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

func TestDecodeRejectsMalformedPayload(t *testing.T) {
	_, err := decode([]byte(`not json`))
	require.Error(t, err)
}
