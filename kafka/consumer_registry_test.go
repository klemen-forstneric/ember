package kafka

import (
	"context"
	"testing"
	"time"
)

func TestKafkaReaderExposesCapAndBackoff(t *testing.T) {
	r := kafkaReader{maxDeliveries: 3, capped: true, backoff: 2 * time.Second}

	if limit, capped := r.MaxDeliveries(); limit != 3 || !capped {
		t.Errorf("MaxDeliveries: got (%d, %v), want (3, true)", limit, capped)
	}
	if r.RetryBackoff() != 2*time.Second {
		t.Errorf("RetryBackoff: got %v, want 2s", r.RetryBackoff())
	}
}

func TestConsumerRegistryUnknownSubscriptionErrors(t *testing.T) {
	reg := NewConsumerRegistry([]string{"localhost:9092"}, map[string]SubscriptionConfig{})
	if _, err := reg.Get(context.Background(), "nope"); err == nil {
		t.Fatal("expected an error for an unknown subscription")
	}
}

func TestConsumerRegistryEmptyTopicsErrors(t *testing.T) {
	reg := NewConsumerRegistry([]string{"localhost:9092"}, map[string]SubscriptionConfig{
		"projector": {}, // no topics configured
	})
	if _, err := reg.Get(context.Background(), "projector"); err == nil {
		t.Fatal("expected an error when a subscription configures no topics")
	}
}

func TestConsumerRegistryGetReturnsConfiguredReader(t *testing.T) {
	reg := NewConsumerRegistry([]string{"localhost:9092"}, map[string]SubscriptionConfig{
		"projector": {Topics: []string{"orders"}, MaxDeliveries: 5},
	})

	r, err := reg.Get(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	if limit, capped := r.MaxDeliveries(); limit != 5 || !capped {
		t.Errorf("MaxDeliveries: got (%d, %v), want (5, true)", limit, capped)
	}
	if r.RetryBackoff() != defaultRetryBackoff {
		t.Errorf("RetryBackoff: got %v, want default %v", r.RetryBackoff(), defaultRetryBackoff)
	}
}

func TestConsumerRegistryUncappedWhenNoMaxDeliveries(t *testing.T) {
	reg := NewConsumerRegistry([]string{"localhost:9092"}, map[string]SubscriptionConfig{
		"projector": {Topics: []string{"orders"}},
	})
	r, err := reg.Get(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	if _, capped := r.MaxDeliveries(); capped {
		t.Error("expected uncapped (capped=false) when MaxDeliveries is zero")
	}
}
