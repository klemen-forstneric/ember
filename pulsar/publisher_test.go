package pulsar

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
)

func envelope(eventType, entityID string) []ember.EventEnvelope {
	return []ember.EventEnvelope{{
		ID:        "evt-1",
		EntityID:  entityID,
		Event:     &ember.MarshaledEvent{Type: eventType, Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{MetadataKeyCorrelationID: "corr-1"},
		Timestamp: time.Unix(0, 0).UTC(),
	}}
}

func TestPublishRoutesByEventType(t *testing.T) {
	reg := newFakeProducerRegistry()
	p := NewPublisher(reg)

	if err := p.Publish(context.Background(), envelope("order.created", "e1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	prod, ok := reg.producers["order.created"]
	if !ok {
		t.Fatal("expected a producer resolved for order.created")
	}
	if len(prod.sent) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(prod.sent))
	}
	if prod.sent[0].Key != "e1" {
		t.Errorf("expected message keyed by entity id e1, got %q", prod.sent[0].Key)
	}
}

func TestPublishUnmappedTypeErrors(t *testing.T) {
	reg := newFakeProducerRegistry()
	reg.getErr = errors.New("unmapped event type")
	p := NewPublisher(reg)

	err := p.Publish(context.Background(), envelope("payment.refunded", "e1"))
	if err == nil {
		t.Fatal("expected an error for an unmapped event type")
	}
}

func TestPublishMissingCorrelationIDErrors(t *testing.T) {
	reg := newFakeProducerRegistry()
	p := NewPublisher(reg)

	e := envelope("order.created", "e1")
	e[0].Metadata = ember.Metadata{} // no correlation id

	if err := p.Publish(context.Background(), e); err == nil {
		t.Fatal("expected an error for missing correlation id")
	}
}

func TestPublishAggregatesSendErrors(t *testing.T) {
	reg := newFakeProducerRegistry()
	reg.producers["order.created"] = &fakeProducer{sendErr: errors.New("boom")}
	p := NewPublisher(reg)

	err := p.Publish(context.Background(), envelope("order.created", "e1"))
	if err == nil {
		t.Fatal("expected the aggregated send error")
	}
}

func TestPublishEmptyIsNoop(t *testing.T) {
	reg := newFakeProducerRegistry()
	p := NewPublisher(reg)
	if err := p.Publish(context.Background(), []ember.EventEnvelope{}); err != nil {
		t.Fatalf("expected nil for empty publish, got %v", err)
	}
}

func TestPublisherCloseClosesRegistry(t *testing.T) {
	reg := newFakeProducerRegistry()
	p := NewPublisher(reg)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if reg.closeCalls != 1 {
		t.Errorf("expected registry Close called once, got %d", reg.closeCalls)
	}
}
