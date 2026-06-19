package pulsar

import (
	"encoding/json"
	"time"

	"github.com/klemen-forstneric/ember"
)

// The delivery-count keys (current_delivery, max_deliveries) live in the core
// ember package; the subscriber stamps them via ember.MetadataKey*.
const (
	MetadataKeyCorrelationID ember.MetadataKey = "correlation_id"
)

// message is the wire envelope serialized to and from the Pulsar payload.
type message struct {
	ID            string          `json:"event_id"`
	CorrelationID string          `json:"correlation_id"`
	EntityID      string          `json:"entity_id"`
	Type          string          `json:"type"`
	Data          json.RawMessage `json:"data"`
	Metadata      ember.Metadata  `json:"metadata"`
	PublishedAt   time.Time       `json:"published_at"`
}
