# Kafka Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Kafka implementation of ember's `Publisher` and `Transport` (subscriber) in a new `kafka/` package, mirroring the `pulsar/` package's structure while correctly handling Kafka's cumulative-offset delivery model.

**Architecture:** A thin `Publisher` over a single multi-topic `*kafka.Writer` routing by event type, and a `Subscriber` (satisfies `ember.Transport`) over one consumer-group `*kafka.Reader` per subscription. The Subscriber owns an offset/retry engine: a per-partition tracker that records delivery attempts and finished offsets and advances a cumulative commit watermark only across contiguous finished offsets — making per-message `Ack`/`Nack` correct under any downstream consumer and under rebalances. A per-subscription `MaxDeliveries` cap drops poison messages (commit-past) so they never stall a partition.

**Tech Stack:** Go, `github.com/segmentio/kafka-go` (pure Go), `github.com/klemen-forstneric/ember` root package. Tests use in-package fakes — no real broker.

**Reference:** Spec at `docs/superpowers/specs/2026-06-03-kafka-transport-design.md`. Mirror conventions from the existing `pulsar/` package (`pulsar/message.go`, `pulsar/publisher.go`, `pulsar/subscriber.go`, `pulsar/consumer_registry.go`, and the `*_test.go` files).

---

## File Structure

- `kafka/message.go` — wire envelope `message` struct + metadata key constants (mirrors `pulsar/message.go`).
- `kafka/publisher.go` — `writer` interface + `Publisher` (route table + single writer).
- `kafka/consumer_registry.go` — `reader` interface, `kafkaReader` adapter, `SubscriptionConfig`, real `ConsumerRegistry`.
- `kafka/offset_tracker.go` — `partitionKey`, `partition`, `offsetTracker` (the pure cumulative-commit logic).
- `kafka/subscriber.go` — `consumerRegistry` interface + `Subscriber` + offset/retry engine.
- `kafka/message_test.go`, `kafka/publisher_test.go`, `kafka/consumer_registry_test.go`, `kafka/offset_tracker_test.go`, `kafka/subscriber_test.go`, `kafka/fakes_test.go`.

---

## Task 1: Add the kafka-go dependency and the wire envelope

**Files:**
- Modify: `go.mod`, `go.sum`
- Create: `kafka/message.go`
- Test: `kafka/message_test.go`

- [ ] **Step 1: Add the kafka-go dependency**

Run:
```bash
go get github.com/segmentio/kafka-go@latest
```
Expected: `go.mod`/`go.sum` updated with a `github.com/segmentio/kafka-go vX.Y.Z` require line (no longer indirect once the package imports it in later steps).

- [ ] **Step 2: Write the failing wire-format round-trip test**

Create `kafka/message_test.go`:
```go
package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
)

func TestMessageJSONRoundTrip(t *testing.T) {
	in := message{
		ID:            "evt-1",
		CorrelationID: "corr-1",
		EntityID:      "e1",
		Type:          "order.created",
		Data:          []byte(`{"k":"v"}`),
		Metadata:      ember.Metadata{MetadataKeyCorrelationID: "corr-1"},
		PublishedAt:   time.Unix(0, 0).UTC(),
	}

	raw, err := json.Marshal(&in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out message
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.ID != in.ID || out.CorrelationID != in.CorrelationID ||
		out.EntityID != in.EntityID || out.Type != in.Type {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
	if string(out.Data) != string(in.Data) {
		t.Errorf("data mismatch: got %s", out.Data)
	}
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `go test ./kafka/ -run TestMessageJSONRoundTrip -v`
Expected: build failure — `undefined: message`, `undefined: MetadataKeyCorrelationID`.

- [ ] **Step 4: Create `kafka/message.go`**

```go
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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test ./kafka/ -run TestMessageJSONRoundTrip -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum kafka/message.go kafka/message_test.go
git commit -m "feat(kafka): add kafka-go dep and wire envelope

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Publisher

**Files:**
- Create: `kafka/publisher.go`, `kafka/fakes_test.go`, `kafka/publisher_test.go`

- [ ] **Step 1: Write the fakes the Publisher tests need**

Create `kafka/fakes_test.go` (this file will be extended in Task 5; start with the writer fake):
```go
package kafka

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

// fakeWriter records every WriteMessages call so tests can assert routing and
// that a multi-topic publish is a single batched call.
type fakeWriter struct {
	mu      sync.Mutex
	written []kafka.Message
	calls   int
	err     error
	closed  bool
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.err != nil {
		return w.err
	}
	w.written = append(w.written, msgs...)
	return nil
}

func (w *fakeWriter) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}
```

- [ ] **Step 2: Write the failing Publisher tests**

Create `kafka/publisher_test.go`:
```go
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

	if err := p.Publish(context.Background(), envelope("order.created", "e1")); err != nil {
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
		envelope("order.created", "e1"),
		envelope("payment.settled", "e2"),
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

	if err := p.Publish(context.Background(), envelope("payment.refunded", "e1")); err == nil {
		t.Fatal("expected an error for an unmapped event type")
	}
}

func TestPublishMissingCorrelationIDErrors(t *testing.T) {
	w := &fakeWriter{}
	p := NewPublisher(w, map[string]string{"order.created": "orders"})

	e := envelope("order.created", "e1")
	e.Metadata = ember.Metadata{} // no correlation id

	if err := p.Publish(context.Background(), e); err == nil {
		t.Fatal("expected an error for missing correlation id")
	}
}

func TestPublishPropagatesWriteError(t *testing.T) {
	w := &fakeWriter{err: errors.New("boom")}
	p := NewPublisher(w, map[string]string{"order.created": "orders"})

	if err := p.Publish(context.Background(), envelope("order.created", "e1")); err == nil {
		t.Fatal("expected the write error to propagate")
	}
}

func TestPublishEmptyIsNoop(t *testing.T) {
	w := &fakeWriter{}
	p := NewPublisher(w, map[string]string{})
	if err := p.Publish(context.Background()); err != nil {
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./kafka/ -run TestPublish -v`
Expected: build failure — `undefined: NewPublisher`.

- [ ] **Step 4: Create `kafka/publisher.go`**

```go
package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/klemen-forstneric/ember"
	"github.com/segmentio/kafka-go"
)

// writer is the narrow slice of *kafka.Writer the Publisher needs. A topic-less
// *kafka.Writer satisfies this directly and routes per-message by Message.Topic.
type writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Publisher sends marshaled event envelopes onto Kafka topics, routing each
// event to its topic via the routes table. A single multi-topic writer is used,
// so no per-topic producer registry is needed (unlike the pulsar package).
type Publisher struct {
	w      writer
	routes map[string]string // eventType -> topic
}

func NewPublisher(w writer, routes map[string]string) *Publisher {
	return &Publisher{w: w, routes: routes}
}

func (p *Publisher) Publish(ctx context.Context, envelopes ...ember.EventEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, 0, len(envelopes))
	for _, e := range envelopes {
		correlationID, ok := e.Metadata[MetadataKeyCorrelationID].(string)
		if !ok {
			return fmt.Errorf("invalid metadata, missing key '%v'", MetadataKeyCorrelationID)
		}

		topic, ok := p.routes[e.Event.Type]
		if !ok {
			return fmt.Errorf("no topic configured for event type %q", e.Event.Type)
		}

		payload, err := json.Marshal(&message{
			ID:            e.ID,
			CorrelationID: correlationID,
			EntityID:      e.EntityID,
			Type:          e.Event.Type,
			Data:          e.Event.Data,
			Metadata:      e.Metadata,
			PublishedAt:   e.Timestamp,
		})
		if err != nil {
			return err
		}

		msgs = append(msgs, kafka.Message{
			Topic: topic,
			Key:   []byte(e.EntityID),
			Value: payload,
			Time:  e.Timestamp,
		})
	}

	return p.w.WriteMessages(ctx, msgs...)
}

func (p *Publisher) Close() error {
	return p.w.Close()
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./kafka/ -run TestPublish -v`
Expected: PASS (all `TestPublish*` and `TestPublisherCloseClosesWriter`).

- [ ] **Step 6: Commit**

```bash
git add kafka/publisher.go kafka/publisher_test.go kafka/fakes_test.go
git commit -m "feat(kafka): add Publisher routing events to topics

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Reader interface, adapter, and ConsumerRegistry

**Files:**
- Create: `kafka/consumer_registry.go`, `kafka/consumer_registry_test.go`

- [ ] **Step 1: Write the failing tests for the adapter and registry**

Create `kafka/consumer_registry_test.go`:
```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./kafka/ -run 'TestKafkaReader|TestConsumerRegistry' -v`
Expected: build failure — `undefined: kafkaReader`, `undefined: NewConsumerRegistry`, `undefined: defaultRetryBackoff`.

- [ ] **Step 3: Create `kafka/consumer_registry.go`**

```go
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// defaultRetryBackoff is the in-session re-emit delay applied to a nacked
// message when a subscription does not configure its own RetryBackoff.
const defaultRetryBackoff = 500 * time.Millisecond

// reader is the slice of *kafka.Reader the Subscriber needs, plus the
// per-subscription delivery cap and retry backoff. The registry interprets the
// SubscriptionConfig and supplies these, so the Subscriber's engine reads them
// off the reader without depending on the registry's config type.
type reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
	MaxDeliveries() (int, bool) // (cap, capped); capped=false => no cap
	RetryBackoff() time.Duration
}

// kafkaReader adapts a *kafka.Reader to the reader interface by embedding it
// (promoting FetchMessage/CommitMessages/Close) and adding the config-derived
// MaxDeliveries and RetryBackoff.
type kafkaReader struct {
	*kafka.Reader
	maxDeliveries int
	capped        bool
	backoff       time.Duration
}

func (r kafkaReader) MaxDeliveries() (int, bool)  { return r.maxDeliveries, r.capped }
func (r kafkaReader) RetryBackoff() time.Duration { return r.backoff }

// SubscriptionConfig describes how one subscription consumes from Kafka. Run
// replicas with the same GroupID to scale: Kafka distributes the subscription's
// partitions across them.
type SubscriptionConfig struct {
	GroupID       string        // defaults to the subscription name when empty
	Topics        []string      // one or more topics for this subscription
	MaxDeliveries int           // 0 => uncapped (capped=false)
	RetryBackoff  time.Duration // 0 => defaultRetryBackoff
}

// ConsumerRegistry maps a subscription name to a single consumer-group reader.
type ConsumerRegistry struct {
	brokers []string
	config  map[string]SubscriptionConfig

	mu      sync.Mutex
	readers []reader
}

func NewConsumerRegistry(brokers []string, config map[string]SubscriptionConfig) *ConsumerRegistry {
	return &ConsumerRegistry{brokers: brokers, config: config}
}

// Get ignores ctx: kafka.NewReader takes only a config. The parameter is kept
// for interface symmetry.
func (r *ConsumerRegistry) Get(_ context.Context, subscription string) (reader, error) {
	cfg, ok := r.config[subscription]
	if !ok {
		return nil, fmt.Errorf("no consumer config for subscription %q", subscription)
	}

	groupID := cfg.GroupID
	if groupID == "" {
		groupID = subscription
	}

	rc := kafka.ReaderConfig{Brokers: r.brokers, GroupID: groupID}
	// A consumer-group reader takes Topic for a single topic or GroupTopics for
	// several; setting both panics, so pick exactly one.
	if len(cfg.Topics) == 1 {
		rc.Topic = cfg.Topics[0]
	} else {
		rc.GroupTopics = cfg.Topics
	}

	backoff := cfg.RetryBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}

	kr := kafkaReader{
		Reader:        kafka.NewReader(rc),
		maxDeliveries: cfg.MaxDeliveries,
		capped:        cfg.MaxDeliveries > 0,
		backoff:       backoff,
	}

	r.mu.Lock()
	r.readers = append(r.readers, kr)
	r.mu.Unlock()

	return kr, nil
}

func (r *ConsumerRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, rd := range r.readers {
		if err := rd.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./kafka/ -run 'TestKafkaReader|TestConsumerRegistry' -v`
Expected: PASS. (The `*kafka.Reader` is created but never dials a broker until `FetchMessage`; `Close` tears down its background goroutines.)

- [ ] **Step 5: Commit**

```bash
git add kafka/consumer_registry.go kafka/consumer_registry_test.go
git commit -m "feat(kafka): add reader interface and consumer-group registry

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Offset tracker (cumulative-commit watermark)

**Files:**
- Create: `kafka/offset_tracker.go`, `kafka/offset_tracker_test.go`

This is the heart of the transport: pure, mutex-guarded per-partition bookkeeping. Test it hard in isolation.

- [ ] **Step 1: Write the failing offset-tracker tests**

Create `kafka/offset_tracker_test.go`:
```go
package kafka

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func msgAt(partition int, offset int64) kafka.Message {
	return kafka.Message{Topic: "orders", Partition: partition, Offset: offset}
}

func TestTrackerRegisterAndRetryCountAttempts(t *testing.T) {
	tr := newOffsetTracker()
	m := msgAt(0, 5)

	if got := tr.register(m); got != 1 {
		t.Errorf("register attempt: got %d, want 1", got)
	}
	if got := tr.attempt(m); got != 1 {
		t.Errorf("attempt: got %d, want 1", got)
	}
	if got := tr.retry(m); got != 2 {
		t.Errorf("first retry: got %d, want 2", got)
	}
	if got := tr.retry(m); got != 3 {
		t.Errorf("second retry: got %d, want 3", got)
	}
}

func TestTrackerCommitsSingleOffset(t *testing.T) {
	tr := newOffsetTracker()
	m := msgAt(0, 5)
	tr.register(m)

	cm, ok := tr.complete(m)
	if !ok {
		t.Fatal("expected a commit after completing the only outstanding offset")
	}
	if cm.Offset != 5 || cm.Partition != 0 || cm.Topic != "orders" {
		t.Errorf("commit message: got %+v, want offset 5 / partition 0 / orders", cm)
	}
}

func TestTrackerHoldsCommitUntilContiguous(t *testing.T) {
	tr := newOffsetTracker()
	m1, m2, m3 := msgAt(0, 1), msgAt(0, 2), msgAt(0, 3)
	tr.register(m1)
	tr.register(m2)
	tr.register(m3)

	// Completing 3 first must NOT advance the watermark (1 and 2 still pending).
	if _, ok := tr.complete(m3); ok {
		t.Fatal("did not expect a commit while offset 1 and 2 are pending")
	}

	// Completing 1 advances the watermark to 1 only (2 still pending).
	cm, ok := tr.complete(m1)
	if !ok || cm.Offset != 1 {
		t.Fatalf("expected commit at offset 1, got ok=%v offset=%d", ok, cm.Offset)
	}

	// Completing 2 now jumps the watermark across 2 and the already-done 3.
	cm, ok = tr.complete(m2)
	if !ok || cm.Offset != 3 {
		t.Fatalf("expected commit to jump to offset 3, got ok=%v offset=%d", ok, cm.Offset)
	}
}

func TestTrackerPartitionsAreIndependent(t *testing.T) {
	tr := newOffsetTracker()
	p0, p1 := msgAt(0, 10), msgAt(1, 20)
	tr.register(p0)
	tr.register(p1)

	cm, ok := tr.complete(p1)
	if !ok || cm.Partition != 1 || cm.Offset != 20 {
		t.Fatalf("partition 1 commit: ok=%v %+v", ok, cm)
	}
	cm, ok = tr.complete(p0)
	if !ok || cm.Partition != 0 || cm.Offset != 10 {
		t.Fatalf("partition 0 commit: ok=%v %+v", ok, cm)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./kafka/ -run TestTracker -v`
Expected: build failure — `undefined: newOffsetTracker`.

- [ ] **Step 3: Create `kafka/offset_tracker.go`**

```go
package kafka

import (
	"sync"

	"github.com/segmentio/kafka-go"
)

type partitionKey struct {
	topic     string
	partition int
}

// partition tracks, for one Kafka partition, the per-offset delivery attempts
// and which offsets are finished (acked or given up), plus the cursor — the
// lowest offset not yet committed. Kafka commits are cumulative, so the
// committed offset can only advance across a contiguous run of finished offsets.
type partition struct {
	cursor   int64
	started  bool
	done     map[int64]bool
	attempts map[int64]int
}

// offsetTracker owns the per-partition state for one reader. All methods are
// safe for concurrent use: Ack/Nack closures run on downstream worker
// goroutines while the fetch loop registers new offsets.
type offsetTracker struct {
	mu    sync.Mutex
	parts map[partitionKey]*partition
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{parts: map[partitionKey]*partition{}}
}

// part returns the partition state for m, creating it on first use. Caller holds mu.
func (t *offsetTracker) part(m kafka.Message) *partition {
	k := partitionKey{topic: m.Topic, partition: m.Partition}
	p, ok := t.parts[k]
	if !ok {
		p = &partition{done: map[int64]bool{}, attempts: map[int64]int{}}
		t.parts[k] = p
	}
	return p
}

// register records the first delivery of a freshly fetched message. It sets the
// partition cursor on the first message ever seen for that partition (Kafka
// delivers a partition's offsets in increasing order from the committed point,
// so the first fetched offset is the commit baseline) and marks attempt 1.
func (t *offsetTracker) register(m kafka.Message) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.part(m)
	if !p.started {
		p.cursor = m.Offset
		p.started = true
	}
	p.attempts[m.Offset] = 1
	return 1
}

// attempt returns the current 1-based attempt number for m's offset.
func (t *offsetTracker) attempt(m kafka.Message) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.part(m).attempts[m.Offset]
}

// retry increments and returns the attempt number for a redelivery.
func (t *offsetTracker) retry(m kafka.Message) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.part(m)
	p.attempts[m.Offset]++
	return p.attempts[m.Offset]
}

// complete marks m's offset finished (acked or given up) and advances the
// cursor across any contiguous run of finished offsets. It returns the message
// to commit (carrying the highest contiguous finished offset) and true when the
// cursor advanced; otherwise ok is false.
func (t *offsetTracker) complete(m kafka.Message) (kafka.Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.part(m)
	p.done[m.Offset] = true
	delete(p.attempts, m.Offset)

	advanced := false
	for p.done[p.cursor] {
		delete(p.done, p.cursor)
		p.cursor++
		advanced = true
	}
	if !advanced {
		return kafka.Message{}, false
	}
	return kafka.Message{Topic: m.Topic, Partition: m.Partition, Offset: p.cursor - 1}, true
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./kafka/ -run TestTracker -v`
Expected: PASS (all four `TestTracker*`).

- [ ] **Step 5: Commit**

```bash
git add kafka/offset_tracker.go kafka/offset_tracker_test.go
git commit -m "feat(kafka): add per-partition offset tracker

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Subscriber and the offset/retry engine

**Files:**
- Create: `kafka/subscriber.go`
- Modify: `kafka/fakes_test.go` (add the reader and registry fakes)
- Create: `kafka/subscriber_test.go`

- [ ] **Step 1: Extend `kafka/fakes_test.go` with the reader and registry fakes**

Append to `kafka/fakes_test.go`:
```go

// fakeReader drives the Subscriber: push messages onto in, and the manager
// goroutine reads them off FetchMessage. CommitMessages are recorded so tests
// can assert the cumulative-commit watermark.
type fakeReader struct {
	in        chan kafka.Message
	mu        sync.Mutex
	committed []kafka.Message
	closed    bool
	maxDel    int
	capped    bool
	backoff   time.Duration
}

func newFakeReader(maxDeliveries int, capped bool) *fakeReader {
	return &fakeReader{
		in:      make(chan kafka.Message, 16),
		maxDel:  maxDeliveries,
		capped:  capped,
		backoff: time.Millisecond,
	}
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case m := <-r.in:
		return m, nil
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	r.committed = append(r.committed, msgs...)
	r.mu.Unlock()
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

func (r *fakeReader) MaxDeliveries() (int, bool)  { return r.maxDel, r.capped }
func (r *fakeReader) RetryBackoff() time.Duration { return r.backoff }

func (r *fakeReader) commits() []kafka.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]kafka.Message, len(r.committed))
	copy(out, r.committed)
	return out
}

// fakeConsumerRegistry maps subscription name -> reader.
type fakeConsumerRegistry struct {
	mu         sync.Mutex
	readers    map[string]reader
	getErr     error
	closeCalls int
}

func (f *fakeConsumerRegistry) Get(_ context.Context, subscription string) (reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	r, ok := f.readers[subscription]
	if !ok {
		return nil, fmt.Errorf("unknown subscription %q", subscription)
	}
	return r, nil
}

func (f *fakeConsumerRegistry) Close() error {
	f.mu.Lock()
	f.closeCalls++
	f.mu.Unlock()
	return nil
}
```

Then update the import block at the top of `kafka/fakes_test.go` to include `fmt` and `time`:
```go
import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)
```

- [ ] **Step 2: Write the failing Subscriber tests**

Create `kafka/subscriber_test.go`:
```go
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

	// Receive all three before acking, keyed by offset via Timestamp-free EntityID.
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./kafka/ -run 'TestSubscribe|TestStop' -v`
Expected: build failure — `undefined: NewSubscriber`.

- [ ] **Step 4: Create `kafka/subscriber.go`**

```go
package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/segmentio/kafka-go"
)

// Subscriber is the Kafka implementation of ember.Transport. It resolves a
// subscription to one consumer-group reader and runs an offset/retry engine
// that makes per-message Ack/Nack correct under any downstream consumer.
var _ ember.Transport = (*Subscriber)(nil)

// consumerRegistry resolves the reader for a subscription name. Get returns an
// error for an unknown subscription.
type consumerRegistry interface {
	Get(ctx context.Context, subscription string) (reader, error)
	Close() error
}

type Subscriber struct {
	registry consumerRegistry
	logger   ember.LoggerCtx

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewSubscriber(r consumerRegistry, l ember.LoggerCtx) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())
	return &Subscriber{registry: r, logger: l, ctx: ctx, cancel: cancel}
}

// Subscribe ignores the caller's ctx for lifecycle: the Subscriber's own ctx
// (cancelled by Stop) governs the fetch loop and retry goroutines, matching the
// pulsar transport's shutdown-channel model.
func (s *Subscriber) Subscribe(_ context.Context, name string) (<-chan ember.AckableEventEnvelope, error) {
	r, err := s.registry.Get(s.ctx, name)
	if err != nil {
		return nil, err
	}

	out := make(chan ember.AckableEventEnvelope)

	s.wg.Add(1)
	go s.run(r, out)

	return out, nil
}

// run is the per-reader fetch loop. It pulls one message at a time, registers
// it, and delivers it. FetchMessage returns an error when s.ctx is cancelled
// (Stop) or the reader is closed, which ends the loop.
func (s *Subscriber) run(r reader, out chan<- ember.AckableEventEnvelope) {
	defer s.wg.Done()
	tracker := newOffsetTracker()

	for {
		m, err := r.FetchMessage(s.ctx)
		if err != nil {
			return
		}
		tracker.register(m)
		s.deliver(r, tracker, out, m)
	}
}

// deliver unmarshals the payload, stamps metadata for the current attempt, and
// forwards the envelope. A malformed payload is poison (it will fail to
// unmarshal on every redelivery), so it is dropped and committed past rather
// than left to stall the partition.
func (s *Subscriber) deliver(r reader, tracker *offsetTracker, out chan<- ember.AckableEventEnvelope, m kafka.Message) {
	var msg message
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		s.logger.Error(s.ctx, "Could not unmarshal the message; dropping", err,
			"topic", m.Topic, "partition", m.Partition, "offset", m.Offset)
		s.commit(r, tracker, m)
		return
	}

	metadata := msg.Metadata
	if metadata == nil {
		metadata = make(ember.Metadata)
	}
	metadata[MetadataKeyCorrelationID] = msg.CorrelationID
	metadata[MetadataKeyCurrentDelivery] = tracker.attempt(m)
	if limit, capped := r.MaxDeliveries(); capped {
		metadata[MetadataKeyMaxDeliveries] = limit
	}

	envelope := ember.AckableEventEnvelope{
		EventEnvelope: ember.EventEnvelope{
			ID:       msg.ID,
			EntityID: msg.EntityID,
			Event: &ember.MarshaledEvent{
				Type: msg.Type,
				Data: msg.Data,
			},
			Metadata:  metadata,
			Timestamp: msg.PublishedAt,
		},
		Ack:  func() { s.commit(r, tracker, m) },
		Nack: func() { s.nack(r, tracker, out, m) },
	}

	select {
	case out <- envelope:
	case <-s.ctx.Done():
	}
}

// nack either schedules an in-session redelivery after the reader's backoff, or
// — when the delivery cap is reached — drops the message and commits past it so
// a poison message cannot stall the partition.
func (s *Subscriber) nack(r reader, tracker *offsetTracker, out chan<- ember.AckableEventEnvelope, m kafka.Message) {
	if limit, capped := r.MaxDeliveries(); capped && tracker.attempt(m) >= limit {
		s.logger.Warn(s.ctx, "Delivery cap reached; dropping message",
			"topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "max_deliveries", limit)
		s.commit(r, tracker, m)
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-time.After(r.RetryBackoff()):
		case <-s.ctx.Done():
			return
		}
		tracker.retry(m)
		s.deliver(r, tracker, out, m)
	}()
}

// commit advances the cumulative watermark and commits when it moves. A commit
// against a just-revoked partition (after a rebalance) may error; log and move on.
func (s *Subscriber) commit(r reader, tracker *offsetTracker, m kafka.Message) {
	cm, ok := tracker.complete(m)
	if !ok {
		return
	}
	if err := r.CommitMessages(s.ctx, cm); err != nil {
		s.logger.Error(s.ctx, "Could not commit offset", err,
			"topic", cm.Topic, "partition", cm.Partition, "offset", cm.Offset)
	}
}

func (s *Subscriber) Stop() {
	s.cancel()
	s.wg.Wait()
	if err := s.registry.Close(); err != nil {
		s.logger.Error(context.Background(), "Could not close consumer registry", err)
	}
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./kafka/ -run 'TestSubscribe|TestStop' -v`
Expected: PASS (all `TestSubscribe*` and `TestStopClosesRegistry`).

- [ ] **Step 6: Run the full package test suite with the race detector**

Run: `go test ./kafka/ -race -v`
Expected: PASS, no data-race reports. (The engine is exercised concurrently: fetch loop, retry goroutines, and Ack/Nack closures all touch the tracker and `out`.)

- [ ] **Step 7: Commit**

```bash
git add kafka/subscriber.go kafka/fakes_test.go kafka/subscriber_test.go
git commit -m "feat(kafka): add Subscriber with offset/retry engine

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Build the whole module**

Run: `go build ./...`
Expected: no output (success).

- [ ] **Step 2: Vet the whole module**

Run: `go vet ./...`
Expected: no output (no findings).

- [ ] **Step 3: Run the entire test suite with the race detector**

Run: `go test ./... -race`
Expected: all packages PASS (`ok github.com/klemen-forstneric/ember/kafka ...`), no race reports.

- [ ] **Step 4: Confirm go.mod is tidy**

Run: `go mod tidy && git diff --exit-code go.mod go.sum`
Expected: exit code 0 (no changes) — kafka-go is a direct dependency and nothing stale remains. If `go mod tidy` changes the files, commit the result.

---

## Self-Review Notes

- **Spec coverage:** Publisher routing + unmapped-type hard error (Task 2); single multi-topic writer / no producerRegistry (Task 2); consumer-group reader per subscription + GroupID default + Topic/GroupTopics selection (Task 3); per-subscription MaxDeliveries + RetryBackoff (Task 3); offset/retry engine with contiguous-commit watermark (Tasks 4–5); Nack retry-then-commit, cap → drop+commit, malformed → drop+commit (Task 5); same wire envelope + metadata keys (Task 1, asserted in Task 5 stamping tests); `ember.Transport` compile-time assertion (Task 5); Stop drains goroutines + closes registry (Task 5). The `*kafka.Reader`/`*kafka.Writer`-backed edges are the documented untested boundary (no integration tests in scope).
- **Type consistency:** `reader`/`writer` interfaces, `MaxDeliveries() (int, bool)`, `RetryBackoff() time.Duration`, `offsetTracker` method names (`register`/`attempt`/`retry`/`complete`), and `message` field names are used identically across Tasks 1–5 and their fakes.
- **Naming:** `limit` is used instead of `cap` to avoid shadowing the Go builtin.
