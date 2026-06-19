package ember

import "context"

// MetadataKey
type MetadataKey string

const (
	// MetadataKeyCurrentDelivery is the 1-based delivery attempt for the event,
	// stamped by a subscriber from the transport's redelivery count.
	MetadataKeyCurrentDelivery MetadataKey = "current_delivery"
	// MetadataKeyMaxDeliveries is the consumer's delivery cap (its DLQ bound),
	// stamped only when the consumer is capped.
	MetadataKeyMaxDeliveries MetadataKey = "max_deliveries"
)

// Metadata
type Metadata map[MetadataKey]interface{}

// CurrentDelivery returns the 1-based delivery attempt for the event, or 0 when
// absent.
func (m Metadata) CurrentDelivery() int {
	v, _ := m[MetadataKeyCurrentDelivery].(int)
	return v
}

// MaxDeliveries returns the consumer's delivery cap, or 0 when the consumer is
// uncapped or the value is absent.
func (m Metadata) MaxDeliveries() int {
	v, _ := m[MetadataKeyMaxDeliveries].(int)
	return v
}

// IsLastDelivery reports whether this is the final redelivery attempt. It is false
// when the consumer is uncapped (no max-deliveries bound) or the counts are absent.
func (m Metadata) IsLastDelivery() bool {
	limit := m.MaxDeliveries()
	return limit > 0 && m.CurrentDelivery() >= limit
}

// MetadataGetter
type MetadataGetter interface {
	Get(ctx context.Context) (Metadata, error)
}

// NoopMetadataGetter
type NoopMetadataGetter struct {
}

func (NoopMetadataGetter) Get(ctx context.Context) (Metadata, error) {
	return make(Metadata), nil
}
