package wal

import (
	"encoding/json"
	"time"

	"github.com/klemen-forstneric/ember"
)

// message is the WAL payload. EventRepository encodes it and the decoder reads
// it back, so the two must agree byte-for-byte.
type message struct {
	ID        string          `json:"id"`
	EntityID  string          `json:"entity_id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Metadata  ember.Metadata  `json:"metadata,omitempty"`
	Version   uint64          `json:"version"`
	Idx       int             `json:"idx"`
	Timestamp time.Time       `json:"created_at"`
}

func encode(e ember.EventEnvelope) ([]byte, error) {
	return json.Marshal(message{
		ID:        e.ID,
		EntityID:  e.EntityID,
		Type:      e.Event.Type,
		Data:      e.Event.Data,
		Metadata:  e.Metadata,
		Version:   e.Version,
		Idx:       e.Index,
		Timestamp: e.Timestamp,
	})
}

func decode(b []byte) (ember.EventEnvelope, error) {
	var m message
	if err := json.Unmarshal(b, &m); err != nil {
		return ember.EventEnvelope{}, err
	}
	return ember.EventEnvelope{
		ID:        m.ID,
		EntityID:  m.EntityID,
		Event:     &ember.MarshaledEvent{Type: m.Type, Data: m.Data},
		Metadata:  m.Metadata,
		Version:   m.Version,
		Index:     m.Idx,
		Timestamp: m.Timestamp,
	}, nil
}
