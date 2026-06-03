package kafka

import (
	"encoding/json"
	"time"

	"github.com/klemen-forstneric/ember"
)

const (
	MetadataKeyCurrentDelivery ember.MetadataKey = "current_delivery"
	MetadataKeyMaxDeliveries   ember.MetadataKey = "max_deliveries"
	MetadataKeyCorrelationID   ember.MetadataKey = "correlation_id"
)

// message is the wire envelope serialized to and from the Kafka payload.
// It is identical to the pulsar package's envelope so downstream code and
// middleware are transport-agnostic.
type message struct {
	ID            string          `json:"event_id"`
	CorrelationID string          `json:"correlation_id"`
	EntityID      string          `json:"entity_id"`
	Type          string          `json:"type"`
	Data          json.RawMessage `json:"data"`
	Metadata      ember.Metadata  `json:"metadata"`
	PublishedAt   time.Time       `json:"published_at"`
}
