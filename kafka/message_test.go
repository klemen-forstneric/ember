package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
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
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out message
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.ID != in.ID || out.CorrelationID != in.CorrelationID ||
		out.EntityID != in.EntityID || out.Type != in.Type {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
	if string(out.Data) != string(in.Data) {
		t.Errorf("data mismatch: got %s", out.Data)
	}
	if !out.PublishedAt.Equal(in.PublishedAt) {
		t.Errorf("PublishedAt mismatch: got %v, want %v", out.PublishedAt, in.PublishedAt)
	}
	if v := out.Metadata[MetadataKeyCorrelationID]; v != in.Metadata[MetadataKeyCorrelationID] {
		t.Errorf("Metadata mismatch: got %v, want %v", v, in.Metadata[MetadataKeyCorrelationID])
	}
}
