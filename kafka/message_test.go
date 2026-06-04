package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageJSONRoundTrip(t *testing.T) {
	in := message{
		ID:            "evt-1",
		CorrelationID: "corr-1",
		EntityID:      "e1",
		Type:          "order.created",
		Data:          []byte(`{"k":"v"}`),
		Metadata:      ember.Metadata{MetadataKeyCorrelationID: "corr-1"},
		PublishedAt:   time.Unix(0, 0).UTC(),
	}

	raw, err := json.Marshal(&in)
	require.NoError(t, err)

	var out message
	require.NoError(t, json.Unmarshal(raw, &out))

	assert.Equal(t, in.ID, out.ID)
	assert.Equal(t, in.CorrelationID, out.CorrelationID)
	assert.Equal(t, in.EntityID, out.EntityID)
	assert.Equal(t, in.Type, out.Type)
	assert.Equal(t, string(in.Data), string(out.Data))
	assert.True(t, out.PublishedAt.Equal(in.PublishedAt), "PublishedAt mismatch")
	assert.Equal(t, in.Metadata[MetadataKeyCorrelationID], out.Metadata[MetadataKeyCorrelationID])
}
