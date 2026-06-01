package aws

import (
	"encoding/json"
	"time"

	"github.com/klemen-forstneric/ember"
)

const (
	MetadataKeyCorrelationID ember.MetadataKey = "correlation_id"
)

// message
type message struct {
	ID            string          `json:"event_id"`
	CorrelationID string          `json:"correlation_id"`
	EntityID      string          `json:"entity_id"`
	Type          string          `json:"type"`
	Metadata      ember.Metadata  `json:"metadata"`
	Payload       json.RawMessage `json:"payload"`
	PublishedAt   time.Time       `json:"published_at"`
}
