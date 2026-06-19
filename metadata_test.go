package ember

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadataDeliveryAccessors(t *testing.T) {
	t.Run("reads current and max", func(t *testing.T) {
		m := Metadata{
			MetadataKeyCurrentDelivery: 3,
			MetadataKeyMaxDeliveries:   10,
		}
		assert.Equal(t, 3, m.CurrentDelivery())
		assert.Equal(t, 10, m.MaxDeliveries())
	})

	t.Run("absent or wrong type yields zero", func(t *testing.T) {
		assert.Equal(t, 0, Metadata{}.CurrentDelivery())
		assert.Equal(t, 0, Metadata{}.MaxDeliveries())
		assert.Equal(t, 0, Metadata{MetadataKeyCurrentDelivery: "3"}.CurrentDelivery())
	})
}

func TestMetadataIsLastDelivery(t *testing.T) {
	tests := []struct {
		name     string
		meta     Metadata
		wantLast bool
	}{
		{"before last", Metadata{MetadataKeyCurrentDelivery: 3, MetadataKeyMaxDeliveries: 10}, false},
		{"at last", Metadata{MetadataKeyCurrentDelivery: 10, MetadataKeyMaxDeliveries: 10}, true},
		{"past last", Metadata{MetadataKeyCurrentDelivery: 11, MetadataKeyMaxDeliveries: 10}, true},
		{"uncapped (no max)", Metadata{MetadataKeyCurrentDelivery: 99}, false},
		{"empty", Metadata{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantLast, tt.meta.IsLastDelivery())
		})
	}
}
