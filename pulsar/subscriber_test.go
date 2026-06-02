package pulsar

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/klemen-forstneric/ember"
)

// stubMessage is a minimal pulsar.Message: only the methods the Subscriber
// touches return meaningful values.
type stubMessage struct {
	pulsar.Message
	payload     []byte
	redelivered uint32
}

func (m stubMessage) Payload() []byte         { return m.payload }
func (m stubMessage) RedeliveryCount() uint32 { return m.redelivered }

func msgFor(t *testing.T, eventType, entityID, correlationID string, redelivered uint32) pulsar.ConsumerMessage {
	t.Helper()
	payload, err := json.Marshal(&message{
		ID:            "evt-1",
		CorrelationID: correlationID,
		EntityID:      entityID,
		Type:          eventType,
		Data:          []byte(`{"k":"v"}`),
		PublishedAt:   time.Unix(0, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return pulsar.ConsumerMessage{Message: stubMessage{payload: payload, redelivered: redelivered}}
}

func TestSubscribeForwardsAndStampsMetadata(t *testing.T) {
	c := newFakeConsumer()
	reg := &fakeConsumerRegistry{subs: map[string][]subscriptionConsumer{
		"projector": {{consumer: c, maxDeliveries: 5}},
	}}
	s := NewSubscriber(reg, ember.NopLogger)

	out, err := s.Subscribe(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	c.in <- msgFor(t, "order.created", "e1", "corr-1", 2)

	select {
	case env := <-out:
		if env.EntityID != "e1" {
			t.Errorf("entity id: got %q", env.EntityID)
		}
		if got := env.Metadata[MetadataKeyCurrentDelivery]; got != 2 {
			t.Errorf("current delivery: got %v, want 2", got)
		}
		if got := env.Metadata[MetadataKeyMaxDeliveries]; got != 5 {
			t.Errorf("max deliveries: got %v, want 5", got)
		}
		if got := env.Metadata[MetadataKeyCorrelationID]; got != "corr-1" {
			t.Errorf("correlation id: got %v", got)
		}
		env.Ack()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for an envelope")
	}

	s.Stop()
	if len(c.acked) != 1 {
		t.Errorf("expected 1 ack, got %d", len(c.acked))
	}
}

func TestSubscribeFansInMultipleConsumers(t *testing.T) {
	a, b := newFakeConsumer(), newFakeConsumer()
	reg := &fakeConsumerRegistry{subs: map[string][]subscriptionConsumer{
		"projector": {{consumer: a, maxDeliveries: 1}, {consumer: b, maxDeliveries: 1}},
	}}
	s := NewSubscriber(reg, ember.NopLogger)

	out, err := s.Subscribe(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	a.in <- msgFor(t, "order.created", "e1", "c", 0)
	b.in <- msgFor(t, "order.created", "e2", "c", 0)

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case env := <-out:
			seen[env.EntityID] = true
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}
	if !seen["e1"] || !seen["e2"] {
		t.Errorf("expected envelopes from both consumers, saw %v", seen)
	}
	s.Stop()
}

func TestSubscribeUnknownSubscriptionErrors(t *testing.T) {
	reg := &fakeConsumerRegistry{subs: map[string][]subscriptionConsumer{}}
	s := NewSubscriber(reg, ember.NopLogger)
	if _, err := s.Subscribe(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for unknown subscription")
	}
}

func TestStopClosesRegistry(t *testing.T) {
	reg := &fakeConsumerRegistry{subs: map[string][]subscriptionConsumer{
		"projector": {{consumer: newFakeConsumer(), maxDeliveries: 1}},
	}}
	s := NewSubscriber(reg, ember.NopLogger)
	if _, err := s.Subscribe(context.Background(), "projector"); err != nil {
		t.Fatal(err)
	}
	s.Stop()
	if reg.closeCalls != 1 {
		t.Errorf("expected registry Close called once, got %d", reg.closeCalls)
	}
}
