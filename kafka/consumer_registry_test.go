package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaReaderExposesCapAndBackoff(t *testing.T) {
	r := kafkaReader{maxDeliveries: 3, capped: true, backoff: 2 * time.Second}

	limit, capped := r.MaxDeliveries()
	assert.Equal(t, 3, limit)
	assert.True(t, capped)
	assert.Equal(t, 2*time.Second, r.RetryBackoff())
}

func TestConsumerRegistryUnknownSubscriptionErrors(t *testing.T) {
	reg := NewConsumerRegistry([]string{"localhost:9092"}, map[string]SubscriptionConfig{})
	_, err := reg.Get(context.Background(), "nope")
	assert.Error(t, err)
}

func TestConsumerRegistryEmptyTopicsErrors(t *testing.T) {
	reg := NewConsumerRegistry([]string{"localhost:9092"}, map[string]SubscriptionConfig{
		"projector": {}, // no topics configured
	})
	_, err := reg.Get(context.Background(), "projector")
	assert.Error(t, err)
}

func TestConsumerRegistryGetReturnsConfiguredReader(t *testing.T) {
	reg := NewConsumerRegistry([]string{"localhost:9092"}, map[string]SubscriptionConfig{
		"projector": {Topics: []string{"orders"}, MaxDeliveries: 5},
	})

	r, err := reg.Get(context.Background(), "projector")
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Close() })

	limit, capped := r.MaxDeliveries()
	assert.Equal(t, 5, limit)
	assert.True(t, capped)
	assert.Equal(t, defaultRetryBackoff, r.RetryBackoff())
}

func TestConsumerRegistryUncappedWhenNoMaxDeliveries(t *testing.T) {
	reg := NewConsumerRegistry([]string{"localhost:9092"}, map[string]SubscriptionConfig{
		"projector": {Topics: []string{"orders"}},
	})
	r, err := reg.Get(context.Background(), "projector")
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Close() })

	_, capped := r.MaxDeliveries()
	assert.False(t, capped, "expected uncapped when MaxDeliveries is zero")
}
