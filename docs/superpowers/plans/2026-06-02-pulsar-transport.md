# Pulsar Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the `pulsar` package's `Publisher` and `Subscriber` so they route events to topics by event type, fan multiple consumers into one subscription channel, and shut down cleanly — backed by symmetric `producerRegistry`/`consumerRegistry` seams.

**Architecture:** Routing + creation + caching of Pulsar objects is folded into two registry interfaces (`producerRegistry.Get(eventType)→producer`, `consumerRegistry.Get(subscription)→[]subscriptionConsumer`), each with `Close()`. `Publisher` and `Subscriber` become thin shells over their registry. Tests inject fake registries and fake `producer`/`consumer` values — no real broker. The concrete `*pulsar.Client`-backed registries are the thin untested edge.

**Tech Stack:** Go 1.26, `github.com/apache/pulsar-client-go@v0.19.0`, standard `testing`.

**Spec:** `docs/superpowers/specs/2026-06-02-pulsar-transport-design.md`

**Branch:** `pulsar-transport` (already checked out).

---

## File Structure

- `pulsar/message.go` — unchanged (existing `message` struct + metadata keys).
- `pulsar/registry.go` — **new.** Interface declarations `producer`, `consumer`, `producerRegistry`, `consumerRegistry`, and the `subscriptionConsumer` struct. Single home for the package's seams.
- `pulsar/publisher.go` — **rewritten.** `Publisher` over `producerRegistry`.
- `pulsar/subscriber.go` — **rewritten.** `Subscriber` over `consumerRegistry`.
- `pulsar/client_registry.go` — **new.** Concrete `*pulsar.Client`-backed `clientProducerRegistry` and `clientConsumerRegistry`. No unit tests (integration edge).
- `pulsar/fakes_test.go` — **new.** Fake `producer`, `consumer`, `producerRegistry`, `consumerRegistry` shared by tests.
- `pulsar/publisher_test.go` — **new.** Publisher unit tests.
- `pulsar/subscriber_test.go` — **new.** Subscriber unit tests.

A note on the existing draft: `pulsar/publisher.go` and `pulsar/subscriber.go` currently declare their own `producer`/`consumer` interfaces inline. Those declarations move to `registry.go`; the rewritten files must NOT redeclare them (duplicate declaration is a compile error).

---

## Task 1: Seams — interfaces and `subscriptionConsumer`

**Files:**
- Create: `pulsar/registry.go`

- [ ] **Step 1: Write `pulsar/registry.go`**

```go
package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

// producer is the narrow slice of *pulsar.Producer the Publisher needs.
// Close() is void to match the SDK, so *pulsar.Producer satisfies this directly.
type producer interface {
	SendAsync(context.Context, *pulsar.ProducerMessage, func(pulsar.MessageID, *pulsar.ProducerMessage, error))
	Close()
}

// consumer is the narrow slice of *pulsar.Consumer the Subscriber needs.
type consumer interface {
	Chan() <-chan pulsar.ConsumerMessage
	Ack(pulsar.Message) error
	Nack(pulsar.Message)
	Close()
}

// subscriptionConsumer pairs a created consumer with the per-consumer metadata
// the Subscriber stamps onto events, so the raw pulsar.ConsumerOptions never
// leaks past the registry boundary.
type subscriptionConsumer struct {
	consumer      consumer
	maxDeliveries int
}

// producerRegistry resolves the producer for an event type, creating and
// caching it on demand. Get returns an error for an unmapped event type.
type producerRegistry interface {
	Get(ctx context.Context, eventType string) (producer, error)
	Close() error
}

// consumerRegistry resolves the consumers for a subscription name (fan-in:
// one subscription may map to several consumers). Get returns an error for an
// unknown subscription.
type consumerRegistry interface {
	Get(ctx context.Context, subscription string) ([]subscriptionConsumer, error)
	Close() error
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./pulsar/`
Expected: FAIL — `publisher.go` and `subscriber.go` still declare `producer`/`consumer` inline, so you'll see "producer redeclared in this block". That is expected and fixed in Tasks 3–4. (If you prefer a clean build here, this task can be committed together with Tasks 3–4; otherwise proceed.)

- [ ] **Step 3: Commit**

```bash
git add pulsar/registry.go
git commit -m "feat(pulsar): add registry seams and subscriptionConsumer"
```

---

## Task 2: Test fakes

**Files:**
- Create: `pulsar/fakes_test.go`

- [ ] **Step 1: Write `pulsar/fakes_test.go`**

```go
package pulsar

import (
	"context"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

// fakeProducer records every SendAsync call and invokes the callback
// synchronously with sendErr, so tests can assert routing and error handling.
type fakeProducer struct {
	mu       sync.Mutex
	sent     []*pulsar.ProducerMessage
	sendErr  error
	closed   bool
}

func (f *fakeProducer) SendAsync(_ context.Context, m *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	f.mu.Lock()
	f.sent = append(f.sent, m)
	f.mu.Unlock()
	cb(nil, m, f.sendErr)
}

func (f *fakeProducer) Close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

// fakeProducerRegistry maps event type -> fakeProducer. A missing entry yields
// getErr (set it to simulate an unmapped type). It records create-once by
// counting Get calls per event type.
type fakeProducerRegistry struct {
	mu         sync.Mutex
	producers  map[string]*fakeProducer
	getErr     error
	getCalls   map[string]int
	closeCalls int
}

func newFakeProducerRegistry() *fakeProducerRegistry {
	return &fakeProducerRegistry{
		producers: map[string]*fakeProducer{},
		getCalls:  map[string]int{},
	}
}

func (f *fakeProducerRegistry) Get(_ context.Context, eventType string) (producer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls[eventType]++
	if f.getErr != nil {
		return nil, f.getErr
	}
	p, ok := f.producers[eventType]
	if !ok {
		p = &fakeProducer{}
		f.producers[eventType] = p
	}
	return p, nil
}

func (f *fakeProducerRegistry) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	for _, p := range f.producers {
		p.Close()
	}
	return nil
}

// fakeConsumer drives the Subscriber: push messages onto in, and the
// Subscriber's goroutine reads them off Chan(). Ack/Nack record the message.
type fakeConsumer struct {
	in     chan pulsar.ConsumerMessage
	mu     sync.Mutex
	acked  []pulsar.Message
	nacked []pulsar.Message
	closed bool
}

func newFakeConsumer() *fakeConsumer {
	return &fakeConsumer{in: make(chan pulsar.ConsumerMessage, 8)}
}

func (f *fakeConsumer) Chan() <-chan pulsar.ConsumerMessage { return f.in }

func (f *fakeConsumer) Ack(m pulsar.Message) error {
	f.mu.Lock()
	f.acked = append(f.acked, m)
	f.mu.Unlock()
	return nil
}

func (f *fakeConsumer) Nack(m pulsar.Message) {
	f.mu.Lock()
	f.nacked = append(f.nacked, m)
	f.mu.Unlock()
}

func (f *fakeConsumer) Close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

// fakeConsumerRegistry maps subscription name -> consumers to fan in.
type fakeConsumerRegistry struct {
	mu         sync.Mutex
	subs       map[string][]subscriptionConsumer
	getErr     error
	closeCalls int
}

func (f *fakeConsumerRegistry) Get(_ context.Context, subscription string) ([]subscriptionConsumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	scs, ok := f.subs[subscription]
	if !ok {
		return nil, fmt.Errorf("unknown subscription %q", subscription)
	}
	return scs, nil
}

func (f *fakeConsumerRegistry) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	return nil
}
```

- [ ] **Step 2: Add the missing import**

`fakeConsumerRegistry.Get` uses `fmt.Errorf`. Add `"fmt"` to the import block.

- [ ] **Step 3: Verify it compiles against the seams**

Run: `go vet ./pulsar/` (will still fail on publisher/subscriber redeclare until Tasks 3–4; the fakes themselves must have no errors of their own — re-check after Task 4).

- [ ] **Step 4: Commit**

```bash
git add pulsar/fakes_test.go
git commit -m "test(pulsar): add fake producer/consumer and registries"
```

---

## Task 3: Publisher

**Files:**
- Rewrite: `pulsar/publisher.go`
- Test: `pulsar/publisher_test.go`

- [ ] **Step 1: Write the failing tests in `pulsar/publisher_test.go`**

```go
package pulsar

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
)

func envelope(eventType, entityID string) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:       "evt-1",
		EntityID: entityID,
		Event:    &ember.MarshaledEvent{Type: eventType, Data: []byte(`{"k":"v"}`)},
		Metadata: ember.Metadata{MetadataKeyCorrelationID: "corr-1"},
		Timestamp: time.Unix(0, 0).UTC(),
	}
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
	e.Metadata = ember.Metadata{} // no correlation id

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
	if err := p.Publish(context.Background()); err != nil {
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
```

- [ ] **Step 2: Rewrite `pulsar/publisher.go`**

```go
package pulsar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/klemen-forstneric/ember"
)

// Publisher sends marshaled event envelopes onto Pulsar topics, routing each
// event to its topic via the producerRegistry.
type Publisher struct {
	registry producerRegistry
}

func NewPublisher(r producerRegistry) *Publisher {
	return &Publisher{registry: r}
}

func (p *Publisher) Publish(ctx context.Context, envelopes ...ember.EventEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	type pending struct {
		prod producer
		msg  *pulsar.ProducerMessage
	}
	prepared := make([]pending, 0, len(envelopes))

	for _, e := range envelopes {
		correlationID, ok := e.Metadata[MetadataKeyCorrelationID].(string)
		if !ok {
			return fmt.Errorf("invalid metadata, missing key '%v'", MetadataKeyCorrelationID)
		}

		prod, err := p.registry.Get(ctx, e.Event.Type)
		if err != nil {
			return err
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

		prepared = append(prepared, pending{
			prod: prod,
			msg: &pulsar.ProducerMessage{
				Key:       e.EntityID,
				Payload:   payload,
				EventTime: e.Timestamp,
			},
		})
	}

	var wg sync.WaitGroup
	ch := make(chan error, len(prepared))
	for _, pm := range prepared {
		wg.Add(1)
		pm.prod.SendAsync(ctx, pm.msg, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
			ch <- err
			wg.Done()
		})
	}
	wg.Wait()
	close(ch)

	var errs []error
	for err := range ch {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close releases the producers held by the registry.
func (p *Publisher) Close() error {
	return p.registry.Close()
}
```

- [ ] **Step 3: Run the Publisher tests, expect FAIL then PASS**

Run: `go test ./pulsar/ -run TestPublish -v`
Expected: the package won't compile until Task 4 removes the inline `consumer` decl from the old `subscriber.go`. If `subscriber.go` still holds the old code, temporarily it coexists (the OLD subscriber.go references `s.consumerFactory` and does not compile). **Therefore Tasks 3 and 4 must be committed together** — implement Task 4 before running. After Task 4: `go test ./pulsar/ -run TestPublish -v` → PASS.

- [ ] **Step 4: Commit (jointly with Task 4 — see Task 4 Step 5)**

---

## Task 4: Subscriber

**Files:**
- Rewrite: `pulsar/subscriber.go`
- Test: `pulsar/subscriber_test.go`

- [ ] **Step 1: Write the failing tests in `pulsar/subscriber_test.go`**

```go
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

func (m stubMessage) Payload() []byte        { return m.payload }
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
```

- [ ] **Step 2: Rewrite `pulsar/subscriber.go`**

```go
package pulsar

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/klemen-forstneric/ember"
)

// Subscriber is the Pulsar implementation of ember.Transport. It resolves a
// subscription name to one or more consumers via the registry and fans their
// messages into a single channel.
type Subscriber struct {
	registry consumerRegistry
	logger   ember.LoggerCtx

	shutdown chan struct{}
	wg       sync.WaitGroup
}

func NewSubscriber(r consumerRegistry, l ember.LoggerCtx) *Subscriber {
	return &Subscriber{
		registry: r,
		logger:   l,
		shutdown: make(chan struct{}),
	}
}

func (s *Subscriber) Subscribe(ctx context.Context, name string) (<-chan ember.AckableEventEnvelope, error) {
	scs, err := s.registry.Get(ctx, name)
	if err != nil {
		return nil, err
	}

	out := make(chan ember.AckableEventEnvelope)

	for _, sc := range scs {
		s.wg.Add(1)
		go func(sc subscriptionConsumer) {
			defer s.wg.Done()
			for {
				select {
				case cmsg := <-sc.consumer.Chan():
					var m message
					if err := json.Unmarshal(cmsg.Payload(), &m); err != nil {
						s.logger.Error(ctx, "Could not unmarshal the message", err)
						continue
					}

					metadata := m.Metadata
					if metadata == nil {
						metadata = make(ember.Metadata)
					}
					metadata[MetadataKeyCurrentDelivery] = int(cmsg.RedeliveryCount())
					metadata[MetadataKeyMaxDeliveries] = sc.maxDeliveries
					metadata[MetadataKeyCorrelationID] = m.CorrelationID

					msg := cmsg.Message
					envelope := ember.AckableEventEnvelope{
						EventEnvelope: ember.EventEnvelope{
							ID:       m.ID,
							EntityID: m.EntityID,
							Event:    &ember.MarshaledEvent{Type: m.Type, Data: m.Data},
							Metadata:  metadata,
							Timestamp: m.PublishedAt,
						},
						Ack: func() {
							if err := sc.consumer.Ack(msg); err != nil {
								s.logger.Error(ctx, "Could not acknowledge the event", err)
							}
						},
						Nack: func() { sc.consumer.Nack(msg) },
					}

					select {
					case out <- envelope:
					case <-s.shutdown:
						return
					}
				case <-s.shutdown:
					return
				}
			}
		}(sc)
	}

	return out, nil
}

// Stop signals all forwarding goroutines to exit, waits for them to drain, then
// releases the consumers via the registry. The out channels are intentionally
// not closed: the downstream ember.Consumer terminates via its own Stop()/ctx.
func (s *Subscriber) Stop() {
	close(s.shutdown)
	s.wg.Wait()
	if err := s.registry.Close(); err != nil {
		s.logger.Error(context.Background(), "Could not close consumer registry", err)
	}
}
```

- [ ] **Step 3: Run the full package test suite**

Run: `go test ./pulsar/ -v`
Expected: all Publisher and Subscriber tests PASS. (This is the first point the package compiles cleanly, since both rewritten files now exist.)

- [ ] **Step 4: Vet**

Run: `go vet ./pulsar/`
Expected: no output.

- [ ] **Step 5: Commit Tasks 3 + 4 together**

```bash
git add pulsar/publisher.go pulsar/publisher_test.go pulsar/subscriber.go pulsar/subscriber_test.go
git commit -m "feat(pulsar): route Publisher by event type and fan-in Subscriber over registries"
```

---

## Task 5: Concrete `*pulsar.Client`-backed registries

**Files:**
- Create: `pulsar/client_registry.go`

No unit tests — this is the thin SDK edge (integration-test territory, out of scope per the spec). The goal is that it compiles and is the production wiring.

- [ ] **Step 1: Write `pulsar/client_registry.go`**

```go
package pulsar

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

// clientProducerRegistry routes event types to topics and lazily creates +
// caches one *pulsar.Producer per topic.
type clientProducerRegistry struct {
	client pulsar.Client
	routes map[string]string // eventType -> topic

	mu        sync.Mutex
	producers map[string]producer // topic -> producer
}

func NewProducerRegistry(client pulsar.Client, routes map[string]string) *clientProducerRegistry {
	return &clientProducerRegistry{
		client:    client,
		routes:    routes,
		producers: map[string]producer{},
	}
}

func (r *clientProducerRegistry) Get(_ context.Context, eventType string) (producer, error) {
	topic, ok := r.routes[eventType]
	if !ok {
		return nil, fmt.Errorf("no topic configured for event type %q", eventType)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.producers[topic]; ok {
		return p, nil
	}
	p, err := r.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		return nil, fmt.Errorf("could not create producer for topic %q: %w", topic, err)
	}
	r.producers[topic] = p
	return p, nil
}

func (r *clientProducerRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.producers {
		p.Close()
	}
	return nil
}

// clientConsumerRegistry maps a subscription name to one or more consumers,
// created eagerly from the configured pulsar.ConsumerOptions on Get.
type clientConsumerRegistry struct {
	client pulsar.Client
	config map[string][]pulsar.ConsumerOptions

	mu        sync.Mutex
	created   []consumer
}

func NewConsumerRegistry(client pulsar.Client, config map[string][]pulsar.ConsumerOptions) *clientConsumerRegistry {
	return &clientConsumerRegistry{client: client, config: config}
}

func (r *clientConsumerRegistry) Get(_ context.Context, subscription string) ([]subscriptionConsumer, error) {
	opts, ok := r.config[subscription]
	if !ok {
		return nil, fmt.Errorf("no consumer options configured for subscription %q", subscription)
	}

	scs := make([]subscriptionConsumer, 0, len(opts))
	for _, opt := range opts {
		c, err := r.client.Subscribe(opt)
		if err != nil {
			return nil, fmt.Errorf("could not create consumer for subscription %q: %w", subscription, err)
		}
		r.mu.Lock()
		r.created = append(r.created, c)
		r.mu.Unlock()

		scs = append(scs, subscriptionConsumer{
			consumer:      c,
			maxDeliveries: maxDeliveries(opt),
		})
	}
	return scs, nil
}

func (r *clientConsumerRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.created {
		c.Close()
	}
	return nil
}

// maxDeliveries derives the per-consumer max delivery count, guarding the nil
// DLQ case. It mirrors the broker's "deliveries before DLQ" minus the first
// delivery, matching how the Subscriber stamps current vs. max.
func maxDeliveries(opt pulsar.ConsumerOptions) int {
	if opt.DLQ == nil {
		return 0
	}
	return int(opt.DLQ.MaxDeliveries) - 1
}
```

- [ ] **Step 2: Build and vet the whole module**

Run: `go build ./... && go vet ./...`
Expected: no output.

- [ ] **Step 3: Run the full package test suite again**

Run: `go test ./pulsar/ -v`
Expected: all PASS (no behavior changed; just confirming the new file didn't break compilation).

- [ ] **Step 4: Commit**

```bash
git add pulsar/client_registry.go
git commit -m "feat(pulsar): add pulsar.Client-backed producer/consumer registries"
```

---

## Task 6: Race check and final verification

- [ ] **Step 1: Run the package tests under the race detector**

Run: `go test -race ./pulsar/`
Expected: PASS, no race warnings. (The Subscriber spins goroutines that touch the fakes' message slices and the shutdown channel; this catches send-on-closed-channel or unsynchronized-access regressions.)

- [ ] **Step 2: Run the whole module's tests**

Run: `go test ./...`
Expected: PASS across the module (no other package depends on the pulsar internals; this confirms nothing else broke).

- [ ] **Step 3: Confirm `pulsar.Subscriber` satisfies `ember.Transport`**

Add this compile-time assertion to the bottom of `pulsar/subscriber.go`:

```go
var _ ember.Transport = (*Subscriber)(nil)
```

Run: `go build ./pulsar/`
Expected: no output. (If it fails, the method set drifted from `Subscribe`/`Stop`.)

- [ ] **Step 4: Commit**

```bash
git add pulsar/subscriber.go
git commit -m "test(pulsar): assert Subscriber satisfies ember.Transport"
```

---

## Self-Review Notes (for the planner; not execution steps)

- **Spec coverage:** routing by event type (Task 3 + 5), hard error on unmapped (Task 3 test + Task 5 `Get`), lazy+cached producers (Task 5), `producerRegistry`/`consumerRegistry` symmetric `Get`/`Close` (Task 1), fan-in (Task 4 test), config-in-registry (Task 5), `subscriptionConsumer` + `maxDeliveries` nil-DLQ guard (Task 1 + Task 5 `maxDeliveries`), `Shutdown`→`Stop`/`ember.Transport` (Task 4 + Task 6 assertion), teardown `close;wait;registry.Close` (Task 4), metadata stamping + unmarshal-skip + ack/nack (Task 4 tests). All covered.
- **Ordering caveat:** Task 1's `go build` failing is intentional and documented; Tasks 3+4 are committed jointly because the package only compiles once both rewritten files replace the inline interface declarations.
- **Type consistency:** `producer.Close()`/`consumer.Close()` are void everywhere; registries return `Close() error`; `subscriptionConsumer{consumer, maxDeliveries}` used identically in fakes, real registry, and subscriber.
```
