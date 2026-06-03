package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/segmentio/kafka-go"
)

func kafkaMsgFor(t *testing.T, partition int, offset int64, eventType, entityID, correlationID string) kafka.Message {
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
	return kafka.Message{Topic: "orders", Partition: partition, Offset: offset, Key: []byte(entityID), Value: payload}
}

func TestSubscribeForwardsStampsAndCommitsOnAck(t *testing.T) {
	r := newFakeReader(5, true)
	reg := &fakeConsumerRegistry{readers: map[string]reader{"projector": r}}
	s := NewSubscriber(reg, ember.NopLogger)

	out, err := s.Subscribe(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	r.in <- kafkaMsgFor(t, 0, 7, "order.created", "e1", "corr-1")

	select {
	case env := <-out:
		if env.EntityID != "e1" {
			t.Errorf("entity id: got %q", env.EntityID)
		}
		if got := env.Metadata[MetadataKeyCurrentDelivery]; got != 1 {
			t.Errorf("current delivery: got %v, want 1", got)
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

	commits := r.commits()
	if len(commits) != 1 || commits[0].Offset != 7 {
		t.Errorf("expected a single commit at offset 7, got %+v", commits)
	}
}

func TestSubscribeOmitsMaxDeliveriesWhenUncapped(t *testing.T) {
	r := newFakeReader(0, false)
	reg := &fakeConsumerRegistry{readers: map[string]reader{"projector": r}}
	s := NewSubscriber(reg, ember.NopLogger)

	out, err := s.Subscribe(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	r.in <- kafkaMsgFor(t, 0, 0, "order.created", "e1", "corr-1")

	select {
	case env := <-out:
		if got := env.Metadata[MetadataKeyCurrentDelivery]; got != 1 {
			t.Errorf("current delivery: got %v, want 1", got)
		}
		if _, ok := env.Metadata[MetadataKeyMaxDeliveries]; ok {
			t.Error("max_deliveries should be absent when there is no cap")
		}
		env.Ack()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for an envelope")
	}
	s.Stop()
}

func TestSubscribeCommitsContiguouslyUnderOutOfOrderAcks(t *testing.T) {
	r := newFakeReader(5, true)
	reg := &fakeConsumerRegistry{readers: map[string]reader{"projector": r}}
	s := NewSubscriber(reg, ember.NopLogger)

	out, err := s.Subscribe(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Three messages on one partition, offsets 1,2,3.
	r.in <- kafkaMsgFor(t, 0, 1, "order.created", "e1", "c")
	r.in <- kafkaMsgFor(t, 0, 2, "order.created", "e2", "c")
	r.in <- kafkaMsgFor(t, 0, 3, "order.created", "e3", "c")

	// Receive all three before acking, keyed by entity id.
	envs := map[string]ember.AckableEventEnvelope{}
	for i := 0; i < 3; i++ {
		select {
		case env := <-out:
			envs[env.EntityID] = env
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for envelopes")
		}
	}

	// Ack out of order: e3 (offset 3) first commits nothing; then e1, then e2.
	envs["e3"].Ack()
	if c := r.commits(); len(c) != 0 {
		t.Fatalf("expected no commit after acking offset 3 alone, got %+v", c)
	}
	envs["e1"].Ack()
	envs["e2"].Ack()

	s.Stop()

	commits := r.commits()
	if len(commits) != 2 || commits[0].Offset != 1 || commits[1].Offset != 3 {
		t.Errorf("expected commits [1, 3], got %+v", commits)
	}
}

func TestSubscribeRetriesNackedMessageThenCommits(t *testing.T) {
	r := newFakeReader(3, true)
	reg := &fakeConsumerRegistry{readers: map[string]reader{"projector": r}}
	s := NewSubscriber(reg, ember.NopLogger)

	out, err := s.Subscribe(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	r.in <- kafkaMsgFor(t, 0, 4, "order.created", "e1", "c")

	// First delivery: attempt 1, nack it.
	select {
	case env := <-out:
		if got := env.Metadata[MetadataKeyCurrentDelivery]; got != 1 {
			t.Errorf("first delivery current_delivery: got %v, want 1", got)
		}
		env.Nack()
	case <-time.After(time.Second):
		t.Fatal("timed out on first delivery")
	}

	// Redelivery: attempt 2, ack it.
	select {
	case env := <-out:
		if got := env.Metadata[MetadataKeyCurrentDelivery]; got != 2 {
			t.Errorf("redelivery current_delivery: got %v, want 2", got)
		}
		env.Ack()
	case <-time.After(time.Second):
		t.Fatal("timed out on redelivery")
	}

	s.Stop()

	commits := r.commits()
	if len(commits) != 1 || commits[0].Offset != 4 {
		t.Errorf("expected a single commit at offset 4, got %+v", commits)
	}
}

func TestSubscribeDropsAndCommitsWhenCapReached(t *testing.T) {
	r := newFakeReader(2, true) // cap of 2 deliveries
	reg := &fakeConsumerRegistry{readers: map[string]reader{"projector": r}}
	s := NewSubscriber(reg, ember.NopLogger)

	out, err := s.Subscribe(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	r.in <- kafkaMsgFor(t, 0, 9, "order.created", "e1", "c")

	// Attempt 1 -> nack -> retried.
	(<-out).Nack()
	// Attempt 2 -> nack -> cap reached -> dropped + committed.
	select {
	case env := <-out:
		if got := env.Metadata[MetadataKeyCurrentDelivery]; got != 2 {
			t.Errorf("second delivery current_delivery: got %v, want 2", got)
		}
		env.Nack()
	case <-time.After(time.Second):
		t.Fatal("timed out on second delivery")
	}

	// No third delivery should arrive.
	select {
	case env := <-out:
		t.Fatalf("did not expect a third delivery, got entity %q", env.EntityID)
	case <-time.After(50 * time.Millisecond):
	}

	s.Stop()

	commits := r.commits()
	if len(commits) != 1 || commits[0].Offset != 9 {
		t.Errorf("expected a drop-commit at offset 9, got %+v", commits)
	}
}

func TestSubscribeDropsAndCommitsMalformedPayload(t *testing.T) {
	r := newFakeReader(5, true)
	reg := &fakeConsumerRegistry{readers: map[string]reader{"projector": r}}
	s := NewSubscriber(reg, ember.NopLogger)

	out, err := s.Subscribe(context.Background(), "projector")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	r.in <- kafka.Message{Topic: "orders", Partition: 0, Offset: 11, Value: []byte("not json")}

	// Nothing is delivered to the handler.
	select {
	case env := <-out:
		t.Fatalf("did not expect a delivery for a malformed payload, got %q", env.EntityID)
	case <-time.After(100 * time.Millisecond):
	}

	s.Stop()

	commits := r.commits()
	if len(commits) != 1 || commits[0].Offset != 11 {
		t.Errorf("expected a drop-commit at offset 11, got %+v", commits)
	}
}

func TestSubscribeUnknownSubscriptionErrors(t *testing.T) {
	reg := &fakeConsumerRegistry{readers: map[string]reader{}}
	s := NewSubscriber(reg, ember.NopLogger)
	if _, err := s.Subscribe(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for unknown subscription")
	}
}

func TestSubscribeGetErrorPropagates(t *testing.T) {
	reg := &fakeConsumerRegistry{getErr: errors.New("boom")}
	s := NewSubscriber(reg, ember.NopLogger)
	if _, err := s.Subscribe(context.Background(), "projector"); err == nil {
		t.Fatal("expected the registry Get error to propagate")
	}
}

func TestStopClosesRegistry(t *testing.T) {
	r := newFakeReader(1, true)
	reg := &fakeConsumerRegistry{readers: map[string]reader{"projector": r}}
	s := NewSubscriber(reg, ember.NopLogger)
	if _, err := s.Subscribe(context.Background(), "projector"); err != nil {
		t.Fatal(err)
	}
	s.Stop()
	if reg.closeCalls != 1 {
		t.Errorf("expected registry Close called once, got %d", reg.closeCalls)
	}
}
