package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
)

func envelope(eventType, entityID string) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:        "evt-1",
		EntityID:  entityID,
		Event:     &ember.MarshaledEvent{Type: eventType, Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{MetadataKeyCorrelationID: "corr-1"},
		Timestamp: time.Unix(0, 0).UTC(),
	}
}

func TestPublishRoutesByEventType(t *testing.T) {
	w := &fakeWriter{}
	p := NewPublisher(w, map[string]string{"order.created": "orders"})

	if err := p.Publish(context.Background(), []ember.EventEnvelope{envelope("order.created", "e1")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if len(w.written) != 1 {
		t.Fatalf("expected 1 written message, got %d", len(w.written))
	}
	if w.written[0].Topic != "orders" {
		t.Errorf("topic: got %q, want orders", w.written[0].Topic)
	}
	if string(w.written[0].Key) != "e1" {
		t.Errorf("key: got %q, want e1", w.written[0].Key)
	}
}

func TestPublishMultipleTopicsInOneBatch(t *testing.T) {
	w := &fakeWriter{}
	p := NewPublisher(w, map[string]string{
		"order.created":   "orders",
		"payment.settled": "payments",
	})

	err := p.Publish(context.Background(),
		[]ember.EventEnvelope{
			envelope("order.created", "e1"),
			envelope("payment.settled", "e2"),
		},
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if w.calls != 1 {
		t.Errorf("expected a single WriteMessages call, got %d", w.calls)
	}
	if len(w.written) != 2 {
		t.Fatalf("expected 2 written messages, got %d", len(w.written))
	}
	topics := map[string]bool{w.written[0].Topic: true, w.written[1].Topic: true}
	if !topics["orders"] || !topics["payments"] {
		t.Errorf("expected both topics, got %v", topics)
	}
}

func TestPublishUnmappedTypeErrors(t *testing.T) {
	w := &fakeWriter{}
	p := NewPublisher(w, map[string]string{})

	if err := p.Publish(context.Background(), []ember.EventEnvelope{envelope("payment.refunded", "e1")}); err == nil {
		t.Fatal("expected an error for an unmapped event type")
	}
}

func TestPublishMissingCorrelationIDErrors(t *testing.T) {
	w := &fakeWriter{}
	p := NewPublisher(w, map[string]string{"order.created": "orders"})

	e := envelope("order.created", "e1")
	e.Metadata = ember.Metadata{} // no correlation id

	if err := p.Publish(context.Background(), []ember.EventEnvelope{e}); err == nil {
		t.Fatal("expected an error for missing correlation id")
	}
}

func TestPublishPropagatesWriteError(t *testing.T) {
	w := &fakeWriter{err: errors.New("boom")}
	p := NewPublisher(w, map[string]string{"order.created": "orders"})

	if err := p.Publish(context.Background(), []ember.EventEnvelope{envelope("order.created", "e1")}); err == nil {
		t.Fatal("expected the write error to propagate")
	}
}

func TestPublishEmptyIsNoop(t *testing.T) {
	w := &fakeWriter{}
	p := NewPublisher(w, map[string]string{})
	if err := p.Publish(context.Background(), []ember.EventEnvelope{}); err != nil {
		t.Fatalf("expected nil for empty publish, got %v", err)
	}
	if w.calls != 0 {
		t.Errorf("expected no WriteMessages calls, got %d", w.calls)
	}
}

func TestPublisherCloseClosesWriter(t *testing.T) {
	w := &fakeWriter{}
	p := NewPublisher(w, map[string]string{})
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !w.closed {
		t.Error("expected the writer to be closed")
	}
}
