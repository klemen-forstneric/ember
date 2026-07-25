# ember Event Delivery Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ember's `Notifier` abstraction with a delivery guarantee chosen at construction (`AtLeastOnce` or `BestEffort`) behind a sealed interface, and promote `mongo.Notifier` to a backend-agnostic `ember.Relay`.

**Architecture:** A `Publisher` holds an unexported `guarantee` whose single method `stage` performs durable work inside the caller's transaction and returns a `delivery` closure for anything that must wait for commit. `atLeastOnce` persists to the outbox and returns no delivery (the `Relay` publishes). `bestEffort` persists nothing and returns a delivery that pushes to the `Sink`. `EntitySaver` calls `stage` inside its transaction and runs the returned delivery in its existing post-commit block.

**Tech Stack:** Go 1.26.3, `github.com/klemen-forstneric/ember`, testify (`suite` + `mock`), `github.com/cenkalti/backoff/v4`.

**Spec:** `docs/superpowers/specs/2026-07-25-ember-event-delivery-design.md`

## Global Constraints

- Module is `github.com/klemen-forstneric/ember`. All paths in this plan are relative to the ember repo root.
- Every task must end with `go build ./... && go test ./...` passing. The tree compiles at every commit.
- Tests use `testify/suite` with `mock.Mock` doubles. Test doubles and helpers stay unexported and live in the package's `mocks_test.go` or the canonical `*_test.go` for the unit.
- Keep comments minimal — a terse one-liner only where the code is genuinely non-obvious. No paragraph rationale blocks.
- Conventional Commits for every commit message.
- **Resolved open decision 1:** there is one outbox interface. `EventRepository` grows `ListUnpublished` and `MarkPublished` instead of gaining a sibling, so `AtLeastOnce` requires a drainable outbox at compile time. mongo's package-local `eventRepository` is deleted, not promoted.
- **Resolved open decision 2:** `RetryingSink` defaults `MaxElapsedTime` to `30 * time.Second` when zero. The old `-1` default (zero retries) is dropped.
- This is a breaking library change. Consuming services are not updated by this plan.

---

### Task 1: Move Lock and Locker to core

`Relay` moves to package `ember` in Task 4, but `Locker` currently lives in `ember/middleware`, which imports `ember` — so core cannot reference it. A second interface declared in core would not work either: `redis.Locker.TryLock` returns the concrete `middleware.Lock`, and Go requires identical return types, not structurally compatible ones. Moving the declarations to core and aliasing them in `middleware` makes the two names the same type, so `redis` and `middleware.Idempotent` need no changes.

**Files:**
- Create: `lock.go`
- Modify: `middleware/idempotent.go:13-21`
- Modify: `redis/locker.go` (add compile-time assertion)

**Interfaces:**
- Consumes: nothing.
- Produces: `ember.Lock` interface with `Release(ctx context.Context) error`; `ember.Locker` interface with `TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)`. `middleware.Lock` and `middleware.Locker` become aliases of these. Task 4's `NewRelay` takes an `ember.Locker`.

- [ ] **Step 1: Write the failing assertion**

Add to the end of `redis/locker.go`:

```go
var _ ember.Locker = (*Locker)(nil)
```

And add the import to `redis/locker.go`'s import block:

```go
	"github.com/klemen-forstneric/ember"
```

- [ ] **Step 2: Run build to verify it fails**

Run: `go build ./...`
Expected: FAIL with `undefined: ember.Locker`

- [ ] **Step 3: Create the core declarations**

Create `lock.go`:

```go
package ember

import (
	"context"
	"time"
)

// Lock
type Lock interface {
	Release(ctx context.Context) error
}

// Locker
type Locker interface {
	TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}
```

- [ ] **Step 4: Replace the middleware declarations with aliases**

In `middleware/idempotent.go`, replace lines 13-21:

```go
// Lock
type Lock interface {
	Release(ctx context.Context) error
}

// Locker
type Locker interface {
	TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}
```

with:

```go
type Lock = ember.Lock

type Locker = ember.Locker
```

The `context` and `time` imports in `middleware/idempotent.go` are still used by `Idempotent`'s body and signature — leave the import block alone.

- [ ] **Step 5: Run build and tests**

Run: `go build ./... && go test ./...`
Expected: PASS. `redis` compiles against `ember.Locker`, and `mongo/mocks_test.go`'s `mockLocker` still satisfies it because `middleware.Lock` is now the same type as `ember.Lock`.

- [ ] **Step 6: Commit**

```bash
git add lock.go middleware/idempotent.go redis/locker.go
git commit -m "refactor: move Lock and Locker to core with middleware aliases"
```

---

### Task 2: Replace Transport with Sink and Source

`ext.Transport` (publish side) and `ember.Transport` (subscribe side) are unrelated interfaces sharing a name. They become `ember.Sink` and `ember.Source`.

**Files:**
- Create: `sink.go`
- Modify: `subscriber.go:36-57,60,112`
- Modify: `ext/retrying_notifier.go:22-24,27-28,32`
- Modify: `mongo/notifier.go:11,33,41,58-65,84`
- Modify: `mongo/mocks_test.go:28-32`
- Modify: `mongo/notifier_test.go:36-51` and every `s.transport` reference
- Modify: `kafka/subscriber.go:13,16`
- Modify: `pulsar/subscriber.go:11,14`
- Modify: `kafka/publisher.go` (add assertion)
- Modify: `pulsar/publisher.go` (add assertion)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `ember.Sink` interface with `Publish(ctx context.Context, envelopes []EventEnvelope) error`; `ember.Source` interface with `Subscribe(ctx context.Context, name string) (<-chan AckableEventEnvelope, error)` and `Stop()`. Tasks 3, 4, and 5 all take an `ember.Sink`. `NewSubscriber`'s second parameter becomes a `Source`.

- [ ] **Step 1: Write the failing assertions**

Add to the end of `pulsar/publisher.go`:

```go
var _ ember.Sink = (*Publisher)(nil)
```

Add to the end of `kafka/publisher.go`:

```go
var _ ember.Sink = (*Publisher)(nil)
```

Both files already import `github.com/klemen-forstneric/ember`.

- [ ] **Step 2: Run build to verify it fails**

Run: `go build ./...`
Expected: FAIL with `undefined: ember.Sink`

- [ ] **Step 3: Create Sink**

Create `sink.go`:

```go
package ember

import "context"

// Sink delivers marshaled event envelopes to the broker.
type Sink interface {
	Publish(ctx context.Context, envelopes []EventEnvelope) error
}
```

- [ ] **Step 4: Rename the subscribe-side Transport to Source**

In `subscriber.go`, replace lines 36-40:

```go
// Transport
type Transport interface {
	Subscribe(ctx context.Context, name string) (<-chan AckableEventEnvelope, error)
	Stop()
}
```

with:

```go
// Source receives event envelopes from the broker.
type Source interface {
	Subscribe(ctx context.Context, name string) (<-chan AckableEventEnvelope, error)
	Stop()
}
```

Then in the same file change the struct field, constructor, and `Stop`:

```go
// Subscriber
type Subscriber struct {
	marshaler EventMarshaler
	source    Source
	consumer  Consumer
	logger    LoggerCtx
}

func NewSubscriber(m EventMarshaler, s Source, c Consumer, l LoggerCtx) *Subscriber {
	return &Subscriber{
		marshaler: m,
		source:    s,
		consumer:  c,
		logger:    l,
	}
}
```

In `Subscribe`, line 60 becomes:

```go
	ch, err := s.source.Subscribe(ctx, sub.Name())
```

In `Stop`, line 112 becomes:

```go
	s.source.Stop()
```

- [ ] **Step 5: Update the Source implementations**

In `kafka/subscriber.go`, change the comment on line 13 to read `// Subscriber is the Kafka implementation of ember.Source. It resolves a` and line 16 to:

```go
var _ ember.Source = (*Subscriber)(nil)
```

In `pulsar/subscriber.go`, change the comment on line 11 to read `// Subscriber is the Pulsar implementation of ember.Source. It resolves a` and line 14 to:

```go
var _ ember.Source = (*Subscriber)(nil)
```

- [ ] **Step 6: Point ext at ember.Sink**

In `ext/retrying_notifier.go`, delete the local `Transport` declaration (lines 22-24):

```go
type Transport interface {
	Publish(ctx context.Context, envelopes []ember.EventEnvelope) error
}
```

Change the struct field and constructor parameter to use `ember.Sink`:

```go
type RetryingNotifier struct {
	config RetryingeNotifierConfig
	sink   ember.Sink
	logger ember.LoggerCtx
}

func NewRetryingNotifier(c RetryingeNotifierConfig, s ember.Sink, l ember.LoggerCtx) *RetryingNotifier {
	if c.MaxElapsedTime == 0 {
		// With -1 we disable indefinite retries.
		c.MaxElapsedTime = -1
	}

	return &RetryingNotifier{
		config: c,
		sink:   s,
		logger: l,
	}
}
```

In `Notify`, the `publish` closure becomes:

```go
	publish := func() error {
		attempt++
		return n.sink.Publish(ctx, envelopes)
	}
```

- [ ] **Step 7: Point mongo.Notifier at ember.Sink**

In `mongo/notifier.go`, remove the now-unused `ext` import on line 11, change the struct field on line 33 from `transport ext.Transport` to `sink ember.Sink`, change the constructor signature and body:

```go
func NewNotifier(store eventRepository, sink ember.Sink, locker ember.Locker, logger ember.LoggerCtx, cfg NotifierConfig) *Notifier {
```

and in the returned literal replace `transport: transport,` with `sink: sink,`. Change the `middleware` import usage: the `locker` field type becomes `ember.Locker`, so the `middleware` import is also unused — remove it. In `publishBatch`, line 84 becomes:

```go
		if err := n.sink.Publish(ctx, []ember.EventEnvelope{e}); err != nil {
```

- [ ] **Step 8: Update the mongo test doubles**

In `mongo/mocks_test.go`, rename `mockTransport` to `mockSink` (lines 28-32):

```go
type mockSink struct{ mock.Mock }

func (m *mockSink) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}
```

Change `mockLocker.TryLock`'s return type from `middleware.Lock` to `ember.Lock` and remove the now-unused `middleware` import:

```go
func (m *mockLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (ember.Lock, error) {
	args := m.Called(ctx, key, ttl)
	var lock ember.Lock
	if v := args.Get(0); v != nil {
		lock = v.(ember.Lock)
	}
	return lock, args.Error(1)
}
```

In `mongo/notifier_test.go`, rename the suite field `transport *mockTransport` to `sink *mockSink`, its `SetupTest` assignment to `s.sink = &mockSink{}`, and every `s.transport.On(...)` / `s.transport.AssertExpectations(...)` / `s.transport.AssertNotCalled(...)` to `s.sink....`. There are references in `SetupTest`, `TestPublishBatchPublishesInOrderAndMarks`, `TestPublishBatchPerEntityHeadOfLine`, `TestPublishBatchLogsPublishedAndRetry`, and `TestTickDrainsWhileFullBatch`.

- [ ] **Step 9: Run build and tests**

Run: `go build ./... && go test ./...`
Expected: PASS, all existing suites green.

- [ ] **Step 10: Commit**

```bash
git add sink.go subscriber.go ext/retrying_notifier.go mongo/notifier.go mongo/mocks_test.go mongo/notifier_test.go kafka/subscriber.go kafka/publisher.go pulsar/subscriber.go pulsar/publisher.go
git commit -m "refactor: replace Transport with Sink and Source"
```

---

### Task 3: Turn RetryingNotifier into RetryingSink

Retry is a property of the transport, not a delivery strategy. `RetryingNotifier` becomes a `Sink` decorator that returns an error instead of swallowing it, and gets a `MaxElapsedTime` default that actually retries.

**Files:**
- Create: `ext/retrying_sink.go`
- Create: `ext/mocks_test.go`
- Create: `ext/retrying_sink_test.go`
- Delete: `ext/retrying_notifier.go`

**Interfaces:**
- Consumes: `ember.Sink` from Task 2.
- Produces: `ext.RetryingSinkConfig` struct with fields `InitialInterval`, `MaxInterval`, `MaxElapsedTime` (all `time.Duration`); `ext.NewRetryingSink(c RetryingSinkConfig, s ember.Sink, l ember.LoggerCtx) *ext.RetryingSink` with method `Publish(ctx context.Context, envelopes []ember.EventEnvelope) error`.

- [ ] **Step 1: Write the failing tests**

Create `ext/mocks_test.go`:

```go
package ext

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/klemen-forstneric/ember"
)

type mockSink struct{ mock.Mock }

func (m *mockSink) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

// mockLogger records level+msg per call so tests can assert logging without
// pinning down every variadic kv.
type mockLogger struct{ mock.Mock }

func (m *mockLogger) Debug(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Info(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Warn(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Error(ctx context.Context, msg string, err error, kvs ...interface{}) {
	m.Called(msg, err)
}
```

Create `ext/retrying_sink_test.go`:

```go
package ext

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/klemen-forstneric/ember"
)

func envelopes() []ember.EventEnvelope {
	return []ember.EventEnvelope{{
		ID:        "e1",
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "T", Data: []byte("{}")},
		Timestamp: time.Unix(0, 1).UTC(),
	}}
}

// fastConfig keeps retries sub-millisecond so exhaustion tests stay quick.
func fastConfig() RetryingSinkConfig {
	return RetryingSinkConfig{
		InitialInterval: time.Millisecond,
		MaxInterval:     time.Millisecond,
		MaxElapsedTime:  10 * time.Millisecond,
	}
}

type RetryingSinkSuite struct {
	suite.Suite
	ctx  context.Context
	sink *mockSink
}

func TestRetryingSinkSuite(t *testing.T) { suite.Run(t, new(RetryingSinkSuite)) }

func (s *RetryingSinkSuite) SetupTest() {
	s.ctx = context.Background()
	s.sink = &mockSink{}
}

func (s *RetryingSinkSuite) TearDownTest() {
	s.sink.AssertExpectations(s.T())
}

func (s *RetryingSinkSuite) TestPublishSucceedsWithoutRetry() {
	envs := envelopes()
	s.sink.On("Publish", mock.Anything, envs).Return(nil).Once()
	r := NewRetryingSink(fastConfig(), s.sink, ember.NopLogger)

	err := r.Publish(s.ctx, envs)

	s.Require().NoError(err)
}

func (s *RetryingSinkSuite) TestPublishRetriesThenSucceeds() {
	envs := envelopes()
	// testify consumes Once() expectations in declaration order.
	s.sink.On("Publish", mock.Anything, envs).Return(errors.New("down")).Once()
	s.sink.On("Publish", mock.Anything, envs).Return(nil).Once()
	r := NewRetryingSink(fastConfig(), s.sink, ember.NopLogger)

	err := r.Publish(s.ctx, envs)

	s.Require().NoError(err)
	s.sink.AssertNumberOfCalls(s.T(), "Publish", 2)
}

func (s *RetryingSinkSuite) TestPublishReturnsErrorWhenRetriesExhausted() {
	envs := envelopes()
	s.sink.On("Publish", mock.Anything, envs).Return(errors.New("down"))
	logger := &mockLogger{}
	logger.On("Warn", "Failed to publish events, retrying...")
	logger.On("Error", "Failed to publish events, retries exhausted", mock.Anything).Once()
	r := NewRetryingSink(fastConfig(), s.sink, logger)

	err := r.Publish(s.ctx, envs)

	s.Require().Error(err)
	logger.AssertExpectations(s.T())
}

func (s *RetryingSinkSuite) TestZeroMaxElapsedTimeDefaultsToThirtySeconds() {
	r := NewRetryingSink(RetryingSinkConfig{}, s.sink, ember.NopLogger)

	s.Equal(30*time.Second, r.config.MaxElapsedTime)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./ext/... -v`
Expected: FAIL with `undefined: RetryingSinkConfig` and `undefined: NewRetryingSink`

- [ ] **Step 3: Create RetryingSink**

Create `ext/retrying_sink.go`:

```go
package ext

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/klemen-forstneric/ember"
)

// RetryingSinkConfig
type RetryingSinkConfig struct {
	// InitialInterval is the delay for the first retry.
	InitialInterval time.Duration
	// MaxInterval is the maximum delay between retries.
	MaxInterval time.Duration
	// MaxElapsedTime is the time after which we stop retrying.
	MaxElapsedTime time.Duration
}

// RetryingSink wraps a Sink with exponential-backoff retries.
type RetryingSink struct {
	config RetryingSinkConfig
	sink   ember.Sink
	logger ember.LoggerCtx
}

func NewRetryingSink(c RetryingSinkConfig, s ember.Sink, l ember.LoggerCtx) *RetryingSink {
	if c.MaxElapsedTime == 0 {
		c.MaxElapsedTime = 30 * time.Second
	}
	if l == nil {
		l = ember.NopLogger
	}

	return &RetryingSink{
		config: c,
		sink:   s,
		logger: l,
	}
}

var _ ember.Sink = (*RetryingSink)(nil)

func (r *RetryingSink) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	var attempt int

	publish := func() error {
		attempt++
		return r.sink.Publish(ctx, envelopes)
	}

	b := backoff.NewExponentialBackOff()
	b.InitialInterval = r.config.InitialInterval
	b.MaxInterval = r.config.MaxInterval
	b.MaxElapsedTime = r.config.MaxElapsedTime

	notify := func(err error, delay time.Duration) {
		r.logger.Warn(ctx, "Failed to publish events, retrying...",
			"error", err, "attempt", attempt, "delay", delay)
	}

	if err := backoff.RetryNotify(publish, b, notify); err != nil {
		r.logger.Error(ctx, "Failed to publish events, retries exhausted", err)
		return err
	}

	for _, e := range envelopes {
		elapsed := time.Since(e.Timestamp)

		r.logger.Info(ctx, "Published event", "eventId", e.ID, "type", e.Event.Type,
			"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
			"metadata", e.Metadata, "timestamp", e.Timestamp,
			"elapsed_ms", elapsed.Milliseconds())
	}
	return nil
}
```

- [ ] **Step 4: Delete the old notifier**

```bash
git rm ext/retrying_notifier.go
```

- [ ] **Step 5: Run tests**

Run: `go test ./ext/... -v`
Expected: PASS, four tests in `RetryingSinkSuite`.

- [ ] **Step 6: Run the full build and suite**

Run: `go build ./... && go test ./...`
Expected: PASS. Nothing inside ember referenced `RetryingNotifier`.

- [ ] **Step 7: Commit**

```bash
git add ext/retrying_sink.go ext/retrying_sink_test.go ext/mocks_test.go ext/retrying_notifier.go
git commit -m "refactor(ext): replace RetryingNotifier with RetryingSink"
```

---

### Task 4: Promote mongo.Notifier to ember.Relay

The poller is backend-agnostic — it needs only `ListUnpublished`/`MarkPublished`, a `Sink`, and a `Locker`. Moving it to core lets it drain the postgres outbox too, and deletes the empty `Notify` method that only existed to satisfy `Notifier`.

**Files:**
- Create: `relay.go`
- Modify: `event.go:20-23` (widen `EventRepository`)
- Create: `relay_test.go`
- Modify: `mocks_test.go` (extend `mockEventRepository`, append relay doubles)
- Modify: `mongo/event_repository.go:16` (comment)
- Delete: `mongo/notifier.go`
- Delete: `mongo/notifier_test.go`
- Delete: `mongo/mocks_test.go`

**Interfaces:**
- Consumes: `ember.Sink` (Task 2), `ember.Locker` (Task 1).
- Produces: `ember.EventRepository` widened to `Save(ctx context.Context, envelopes []EventEnvelope) error`, `ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error)`, `MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error`; `ember.RelayConfig` struct with `IdleInterval`, `BatchSize`, `LockKey`, `LockTTL`, `Retention`; `ember.NewRelay(r EventRepository, s Sink, l Locker, log LoggerCtx, cfg RelayConfig) *Relay` with methods `Run(ctx context.Context)`, `Close() error`, and unexported `publishBatch(ctx context.Context) (int, error)` / `tick(ctx context.Context)`.

- [ ] **Step 1: Add the test doubles**

Extend the existing `mockEventRepository` in `mocks_test.go` by appending its two drain methods next to the current `Save`:

```go
func (m *mockEventRepository) ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error) {
	args := m.Called(ctx, limit)
	var envs []EventEnvelope
	if v := args.Get(0); v != nil {
		envs = v.([]EventEnvelope)
	}
	return envs, args.Error(1)
}

func (m *mockEventRepository) MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error {
	return m.Called(ctx, ids, expiresAt).Error(0)
}
```

Then append the remaining doubles to the same file:

```go
// mockSink is a testify mock for Sink.
type mockSink struct {
	mock.Mock
}

func (m *mockSink) Publish(ctx context.Context, envelopes []EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

// mockLocker is a testify mock for Locker.
type mockLocker struct {
	mock.Mock
}

func (m *mockLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error) {
	args := m.Called(ctx, key, ttl)
	var lock Lock
	if v := args.Get(0); v != nil {
		lock = v.(Lock)
	}
	return lock, args.Error(1)
}

// mockLock is a testify mock for Lock.
type mockLock struct {
	mock.Mock
}

func (m *mockLock) Release(ctx context.Context) error { return m.Called(ctx).Error(0) }

// mockLogger records level+msg per call so tests can assert logging without
// pinning down every variadic kv.
type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) Debug(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Info(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Warn(ctx context.Context, msg string, kvs ...interface{}) { m.Called(msg) }

func (m *mockLogger) Error(ctx context.Context, msg string, err error, kvs ...interface{}) {
	m.Called(msg, err)
}
```

Add `"time"` to `mocks_test.go`'s import block.

- [ ] **Step 2: Write the failing test**

Create `relay_test.go`:

```go
package ember

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func evt(id, entityID string) EventEnvelope {
	return EventEnvelope{
		ID:        id,
		EntityID:  entityID,
		Event:     &MarshaledEvent{Type: "T", Data: []byte("{}")},
		Timestamp: time.Unix(0, 1).UTC(),
	}
}

func testRelayConfig() RelayConfig {
	return RelayConfig{
		IdleInterval: time.Millisecond,
		BatchSize:    10,
		LockKey:      "outbox:test",
		LockTTL:      time.Minute,
		Retention:    24 * time.Hour,
	}
}

type RelaySuite struct {
	suite.Suite
	repository *mockEventRepository
	sink       *mockSink
	locker     *mockLocker
	r          *Relay
}

func TestRelaySuite(t *testing.T) {
	suite.Run(t, new(RelaySuite))
}

func (s *RelaySuite) SetupTest() {
	s.repository = &mockEventRepository{}
	s.sink = &mockSink{}
	s.locker = &mockLocker{}
	s.r = NewRelay(s.repository, s.sink, s.locker, NopLogger, testRelayConfig())
}

func (s *RelaySuite) TestPublishBatchPublishesInOrderAndMarks() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	var order []string
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Run(func(a mock.Arguments) {
		envs := a.Get(1).([]EventEnvelope)
		order = append(order, envs[0].ID)
	})
	s.repository.On("MarkPublished", mock.Anything, []string{"e1", "e2", "e3"}, mock.Anything).Return(nil).Once()

	published, err := s.r.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(3, published)
	s.Equal([]string{"e1", "e2", "e3"}, order, "must publish one-at-a-time in seq order")
	s.repository.AssertExpectations(s.T())
}

func (s *RelaySuite) TestPublishBatchPerEntityHeadOfLine() {
	// Seq order: A/e1, A/e2, B/e3. A/e1 fails → A/e2 must be skipped; B/e3 proceeds.
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "A"), evt("e3", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e1"
	})).Return(errors.New("route fail"))
	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e3"
	})).Return(nil)
	// Only e3 is marked; e1 (failed) and e2 (blocked behind e1) stay pending.
	s.repository.On("MarkPublished", mock.Anything, []string{"e3"}, mock.Anything).Return(nil).Once()

	published, err := s.r.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(1, published)
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e2"
	}))
	s.repository.AssertExpectations(s.T())
}

func (s *RelaySuite) TestPublishBatchLogsPublishedAndRetry() {
	batch := []EventEnvelope{evt("e1", "A"), evt("e2", "B")}
	s.repository.On("ListUnpublished", mock.Anything, 10).Return(batch, nil).Once()

	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e1"
	})).Return(nil)
	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(e []EventEnvelope) bool {
		return e[0].ID == "e2"
	})).Return(errors.New("sink down"))
	s.repository.On("MarkPublished", mock.Anything, []string{"e1"}, mock.Anything).Return(nil).Once()

	logger := &mockLogger{}
	logger.On("Info", "Published event").Once()
	logger.On("Warn", "Failed to publish event, will retry").Once()
	s.r = NewRelay(s.repository, s.sink, s.locker, logger, testRelayConfig())

	published, err := s.r.publishBatch(context.Background())

	s.Require().NoError(err)
	s.Equal(1, published)
	logger.AssertExpectations(s.T())
}

func (s *RelaySuite) TestTickNotLeaderDoesNothing() {
	// nil lock → someone else is leader this round.
	s.locker.On("TryLock", mock.Anything, "outbox:test", time.Minute).Return(nil, nil).Once()

	s.r.tick(context.Background())

	s.repository.AssertNotCalled(s.T(), "ListUnpublished", mock.Anything, mock.Anything)
	s.locker.AssertExpectations(s.T())
}

func (s *RelaySuite) TestTickDrainsWhileFullBatch() {
	lock := &mockLock{}
	s.locker.On("TryLock", mock.Anything, "outbox:test", time.Minute).Return(lock, nil).Once()
	lock.On("Release", mock.Anything).Return(nil).Once()

	// cfg.BatchSize is 10. First batch: 10 events (all published) → drain again.
	// Second batch: empty → stop.
	full := make([]EventEnvelope, 10)
	for i := range full {
		full[i] = evt("full", "A")
	}
	s.repository.On("ListUnpublished", mock.Anything, 10).Return(full, nil).Once()
	s.repository.On("ListUnpublished", mock.Anything, 10).Return([]EventEnvelope{}, nil).Once()
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil)
	s.repository.On("MarkPublished", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.r.tick(context.Background())

	s.repository.AssertNumberOfCalls(s.T(), "ListUnpublished", 2)
	lock.AssertExpectations(s.T())
}

func (s *RelaySuite) TestRunStopsOnContextCancel() {
	// Always not-leader so ticks are cheap; Run must still exit on cancel.
	s.locker.On("TryLock", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.r.Run(ctx); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("Run did not return after context cancel")
	}
}

func (s *RelaySuite) TestRunStopsOnClose() {
	// Always not-leader so ticks are cheap; Run must exit once Close is called.
	s.locker.On("TryLock", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	done := make(chan struct{})
	go func() { s.r.Run(context.Background()); close(done) }()

	s.Require().NoError(s.r.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("Run did not return after Close")
	}
}

func (s *RelaySuite) TestCloseIsIdempotent() {
	s.NotPanics(func() {
		s.NoError(s.r.Close())
		s.NoError(s.r.Close())
	})
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test . -run TestRelaySuite -v`
Expected: FAIL with `undefined: RelayConfig` and `undefined: NewRelay`

- [ ] **Step 4: Widen EventRepository**

In `event.go`, replace the `EventRepository` declaration (lines 20-23):

```go
// EventRepository is the outbox: the durable write side plus the drain side the
// Relay uses. AtLeastOnce requires all three, so an outbox nothing can drain is
// not a wireable configuration.
type EventRepository interface {
	Save(ctx context.Context, envelopes []EventEnvelope) error
	ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}
```

`event.go` already imports `context` and `time`.

- [ ] **Step 5: Create Relay**

Create `relay.go`:

```go
package ember

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sync"
	"time"
)

// RelayConfig
type RelayConfig struct {
	IdleInterval time.Duration // idle poll cadence (jittered per replica)
	BatchSize    int           // events fetched per round
	LockKey      string        // redis efficiency-lock key
	LockTTL      time.Duration // lock lease; must exceed a bounded round
	Retention    time.Duration // published_at + Retention → expires_at (TTL)
}

// Relay drains the outbox to the Sink. It is the sole publisher under the
// AtLeastOnce guarantee.
type Relay struct {
	repository EventRepository
	sink       Sink
	locker     Locker
	logger     LoggerCtx
	cfg        RelayConfig
	done       chan struct{}
	closeOnce  sync.Once
}

func NewRelay(r EventRepository, s Sink, l Locker, log LoggerCtx, cfg RelayConfig) *Relay {
	if cfg.IdleInterval <= 0 {
		cfg.IdleInterval = 200 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = 30 * time.Second
	}
	if cfg.Retention <= 0 {
		cfg.Retention = 7 * 24 * time.Hour
	}
	if log == nil {
		log = NopLogger
	}

	return &Relay{
		repository: r,
		sink:       s,
		locker:     l,
		logger:     log,
		cfg:        cfg,
		done:       make(chan struct{}),
	}
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	events, err := r.repository.ListUnpublished(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}

	failed := make(map[string]bool)
	published := make([]string, 0, len(events))
	for i := range events {
		e := events[i]
		if failed[e.EntityID] {
			continue
		}
		if err := r.sink.Publish(ctx, []EventEnvelope{e}); err != nil {
			failed[e.EntityID] = true
			r.logger.Warn(ctx, "Failed to publish event, will retry",
				"error", err, "eventId", e.ID, "type", e.Event.Type, "entity_id", e.EntityID)
			continue
		}
		published = append(published, e.ID)

		elapsed := time.Since(e.Timestamp)
		r.logger.Info(ctx, "Published event", "eventId", e.ID, "type", e.Event.Type,
			"entity_id", e.EntityID, "payload", json.RawMessage(e.Event.Data),
			"metadata", e.Metadata, "timestamp", e.Timestamp,
			"elapsed_ms", elapsed.Milliseconds())
	}

	if len(published) > 0 {
		expiresAt := time.Now().UTC().Add(r.cfg.Retention)
		if err := r.repository.MarkPublished(ctx, published, expiresAt); err != nil {
			return len(published), err
		}
	}
	return len(published), nil
}

func (r *Relay) tick(ctx context.Context) {
	lock, err := r.locker.TryLock(ctx, r.cfg.LockKey, r.cfg.LockTTL)
	if err != nil {
		r.logger.Error(ctx, "Failed to acquire outbox lock", err, "key", r.cfg.LockKey)
		return
	}
	if lock == nil {
		return
	}

	defer func() {
		if err := lock.Release(context.WithoutCancel(ctx)); err != nil {
			r.logger.Warn(ctx, "Failed to release outbox lock", "key", r.cfg.LockKey, "error", err)
		}
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		published, err := r.publishBatch(ctx)
		if err != nil {
			r.logger.Error(ctx, "Failed to drain outbox batch", err)
			return
		}
		if published < r.cfg.BatchSize {
			return
		}
	}
}

func (r *Relay) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		default:
		}
		r.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-time.After(r.interval()):
		}
	}
}

func (r *Relay) interval() time.Duration {
	return r.cfg.IdleInterval + time.Duration(rand.Int64N(int64(r.cfg.IdleInterval)))
}

func (r *Relay) Close() error {
	r.closeOnce.Do(func() { close(r.done) })
	return nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test . -run TestRelaySuite -v`
Expected: PASS, eight tests.

- [ ] **Step 7: Delete the mongo notifier**

```bash
git rm mongo/notifier.go mongo/notifier_test.go mongo/mocks_test.go
```

`mongo/mocks_test.go` exists only for the notifier suite — `event_repository_test.go`, `transactor_test.go`, `ensure_test.go`, `filter_test.go`, and `sort_test.go` use sqlmock-style fixtures instead. Confirm with `go vet ./mongo/...` after deleting; if a remaining suite does reference one of those doubles, restore just that type rather than the whole file.

Then update the stale comment in `mongo/event_repository.go:16` from `// driven by the relay (mongo.Notifier).` to:

```go
// driven by the relay (ember.Relay).
```

- [ ] **Step 8: Run the full build and suite**

Run: `go build ./... && go test ./...`
Expected: PASS. `mongo` retains its `EventRepository`, `EntityRepository`, `Transactor`, and `ensure` suites.

- [ ] **Step 9: Verify both backends satisfy EventRepository**

Add to `mongo/event_repository.go` after the `EventRepository` type declaration:

```go
var _ ember.EventRepository = (*EventRepository)(nil)
```

Add the equivalent to `postgres/event_repository.go` after its `EventRepository` type declaration:

```go
var _ ember.EventRepository = (*EventRepository)(nil)
```

Run: `go build ./...`
Expected: PASS. If either fails, the widened interface in `event.go` does not match the repositories and must be reconciled before continuing.

- [ ] **Step 10: Commit**

```bash
git add relay.go relay_test.go event.go mocks_test.go mongo/ postgres/event_repository.go
git commit -m "refactor: promote mongo.Notifier to backend-agnostic ember.Relay"
```

---

### Task 5: Replace Notifier with a construction-time guarantee

`Publisher` stops doing three jobs. It builds envelopes and delegates to an unexported `guarantee`, chosen by `AtLeastOnce` or `BestEffort`. The `Notifier` interface and the `noop` package disappear.

**Files:**
- Modify: `publisher.go` (full rewrite)
- Create: `publisher_test.go`
- Delete: `notifier.go`
- Delete: `noop/notifier.go`
- Delete: `noop/event_repository.go`

**Interfaces:**
- Consumes: `ember.Sink` (Task 2), `envelopeBuilder` and `EventRepository` (existing, `event.go`).
- Produces: `delivery` (unexported, `func(ctx context.Context) error`); `guarantee` (unexported interface, method `stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error)`); `ember.AtLeastOnce(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler) *Publisher`; `ember.BestEffort(i IDer, s Sink, mg MetadataGetter, m EventMarshaler) *Publisher`; `(*Publisher).Publish(ctx context.Context, events ...Event) error`; unexported `(*Publisher).stage(ctx context.Context, events ...Event) (delivery, error)` used by Task 6; `ember.ErrDeliveryFailed` used by Task 6.

- [ ] **Step 1: Write the failing test**

Create `publisher_test.go`:

```go
package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type PublisherSuite struct {
	suite.Suite
	ctx       context.Context
	repo      *mockEventRepository
	sink      *mockSink
	marshaler *mockEventMarshaler
}

func TestPublisherSuite(t *testing.T) { suite.Run(t, new(PublisherSuite)) }

func (s *PublisherSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEventRepository{}
	s.sink = &mockSink{}
	s.marshaler = &mockEventMarshaler{}
}

func (s *PublisherSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.sink.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
}

func (s *PublisherSuite) atLeastOncePublisher() *Publisher {
	return AtLeastOnce(stubIDer{id: "evt-1"}, s.repo, NoopMetadataGetter{}, s.marshaler)
}

func (s *PublisherSuite) bestEffortPublisher() *Publisher {
	return BestEffort(stubIDer{id: "evt-1"}, s.sink, NoopMetadataGetter{}, s.marshaler)
}

func (s *PublisherSuite) TestAtLeastOncePersistsEnvelopes() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	marshaled := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(marshaled, nil)
	s.repo.On("Save", mock.Anything, mock.MatchedBy(func(envs []EventEnvelope) bool {
		return len(envs) == 1 &&
			envs[0].ID == "evt-1" &&
			envs[0].EntityID == "A" &&
			envs[0].Event == marshaled
	})).Return(nil)

	err := s.atLeastOncePublisher().Publish(s.ctx, evt)

	s.Require().NoError(err)
	// sink is never touched — asserted by TearDownTest.
}

func (s *PublisherSuite) TestAtLeastOnceMarshalError() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(nil, errors.New("boom"))

	err := s.atLeastOncePublisher().Publish(s.ctx, evt)

	s.Require().Error(err)
	// repo.Save must NOT be called — asserted by TearDownTest.
}

func (s *PublisherSuite) TestAtLeastOnceRepositoryError() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	repoErr := errors.New("outbox down")
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)
	s.repo.On("Save", mock.Anything, mock.Anything).Return(repoErr)

	err := s.atLeastOncePublisher().Publish(s.ctx, evt)

	s.Require().ErrorIs(err, repoErr)
}

func (s *PublisherSuite) TestAtLeastOnceStageDefersNothing() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)
	s.repo.On("Save", mock.Anything, mock.Anything).Return(nil)

	d, err := s.atLeastOncePublisher().stage(s.ctx, evt)

	s.Require().NoError(err)
	s.Nil(d, "the Relay delivers; nothing waits on commit")
}

func (s *PublisherSuite) TestBestEffortPublishesToSink() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	marshaled := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(marshaled, nil)
	s.sink.On("Publish", mock.Anything, mock.MatchedBy(func(envs []EventEnvelope) bool {
		return len(envs) == 1 && envs[0].ID == "evt-1" && envs[0].Event == marshaled
	})).Return(nil)

	err := s.bestEffortPublisher().Publish(s.ctx, evt)

	s.Require().NoError(err)
	// repo is never touched — asserted by TearDownTest.
}

func (s *PublisherSuite) TestBestEffortSinkError() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	sinkErr := errors.New("broker down")
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(sinkErr)

	err := s.bestEffortPublisher().Publish(s.ctx, evt)

	s.Require().ErrorIs(err, sinkErr)
}

func (s *PublisherSuite) TestBestEffortStageDefersDelivery() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(&MarshaledEvent{Type: "Created"}, nil)

	d, err := s.bestEffortPublisher().stage(s.ctx, evt)

	s.Require().NoError(err)
	s.Require().NotNil(d, "delivery must wait for commit")
	s.sink.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)

	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()
	s.Require().NoError(d(s.ctx))
}

func (s *PublisherSuite) TestPublishNoEventsIsNoop() {
	// no expectations set anywhere; TearDownTest catches any unexpected call.
	s.Require().NoError(s.atLeastOncePublisher().Publish(s.ctx))
	s.Require().NoError(s.bestEffortPublisher().Publish(s.ctx))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test . -run TestPublisherSuite -v`
Expected: FAIL with `undefined: AtLeastOnce` and `undefined: BestEffort`

- [ ] **Step 3: Rewrite Publisher**

Replace the entire contents of `publisher.go`:

```go
package ember

import (
	"context"
	"errors"
)

// ErrDeliveryFailed marks a delivery that failed after its transaction
// committed. Reachable only under BestEffort.
var ErrDeliveryFailed = errors.New("ember: event delivery failed after commit")

// IDer
type IDer interface {
	ID() string
}

// delivery is work that must run only after the transaction commits.
type delivery func(ctx context.Context) error

// guarantee is the delivery guarantee a Publisher was built with. Unexported,
// so the set of guarantees is closed.
type guarantee interface {
	// stage performs the guarantee's durable work inside the caller's
	// transaction and returns any delivery deferred until after commit.
	stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error)
}

type atLeastOnce struct {
	repo EventRepository
}

func (g atLeastOnce) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	return nil, g.repo.Save(ctx, envelopes)
}

type bestEffort struct {
	sink Sink
}

func (g bestEffort) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
	return func(ctx context.Context) error {
		return g.sink.Publish(ctx, envelopes)
	}, nil
}

// Publisher builds event envelopes and hands them to its guarantee.
type Publisher struct {
	builder   envelopeBuilder
	guarantee guarantee
}

// AtLeastOnce persists envelopes to the outbox inside the caller's transaction.
// A Relay draining that outbox is the sole publisher.
func AtLeastOnce(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler) *Publisher {
	return &Publisher{
		builder:   envelopeBuilder{ider: i, metadata: mg, marshaler: m},
		guarantee: atLeastOnce{repo: r},
	}
}

// BestEffort persists nothing and pushes to the Sink after commit. A crash
// between commit and push loses the event.
func BestEffort(i IDer, s Sink, mg MetadataGetter, m EventMarshaler) *Publisher {
	return &Publisher{
		builder:   envelopeBuilder{ider: i, metadata: mg, marshaler: m},
		guarantee: bestEffort{sink: s},
	}
}

func (p *Publisher) stage(ctx context.Context, events ...Event) (delivery, error) {
	if len(events) == 0 {
		return nil, nil
	}

	envelopes, err := p.builder.build(ctx, events...)
	if err != nil {
		return nil, err
	}
	return p.guarantee.stage(ctx, envelopes)
}

// Publish is the entity-less path: no transaction is in scope, so a deferred
// delivery runs immediately.
func (p *Publisher) Publish(ctx context.Context, events ...Event) error {
	d, err := p.stage(ctx, events...)
	if err != nil || d == nil {
		return err
	}
	return d(ctx)
}
```

- [ ] **Step 4: Delete Notifier and the noop package**

```bash
git rm notifier.go noop/notifier.go noop/event_repository.go
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test . -run TestPublisherSuite -v`
Expected: PASS, eight tests.

- [ ] **Step 6: Run the full build and suite**

Run: `go build ./... && go test ./...`
Expected: PASS. `EventStore` still exists and `EntitySaver` still uses it — that changes in Task 6.

- [ ] **Step 7: Commit**

```bash
git add publisher.go publisher_test.go notifier.go noop/
git commit -m "feat: replace Notifier with AtLeastOnce and BestEffort guarantees"
```

---

### Task 6: Point EntitySaver at Publisher and delete EventStore

`EventStore` was build-plus-persist, which is exactly `atLeastOnce.stage`. `EntitySaver` calls `Publisher.stage` inside its transaction and runs the returned delivery in its existing post-commit block, so one call site serves both guarantees.

**Files:**
- Modify: `saver.go:13-27,29-78`
- Modify: `store.go:11-18`
- Modify: `event.go:87-110` (delete `EventStore`)
- Modify: `mocks_test.go` (append `recordingTransactor`)
- Modify: `saver_test.go:12-42,140-170`
- Modify: `store_test.go:31-32`
- Delete: `event_test.go`

**Interfaces:**
- Consumes: `(*Publisher).stage`, `ErrDeliveryFailed`, `AtLeastOnce`, `BestEffort` (Task 5).
- Produces: `ember.NewEntitySaver(p *Publisher, tx Transactor, bindings ...binder) *EntitySaver`; `ember.NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E], p *Publisher, tx Transactor) *EntityStore[E]`.

- [ ] **Step 1: Add the commit-boundary test double**

Append to `mocks_test.go`:

```go
// recordingTransactor marks the commit boundary so tests can assert that a
// deferred delivery runs after it, which a mock's call order cannot express.
type recordingTransactor struct {
	committed bool
}

func (t *recordingTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	if err := fn(ctx); err != nil {
		return err
	}
	t.committed = true
	return nil
}
```

- [ ] **Step 2: Write the failing tests**

In `saver_test.go`, replace the suite declaration and `SetupTest` (lines 12-42) with:

```go
type EntitySaverSuite struct {
	suite.Suite
	ctx         context.Context
	entityRepo  *mockEntityRepository
	entityMarsh *mockEntityMarshaler[*fakeEntity]
	eventRepo   *mockEventRepository
	eventMarsh  *mockEventMarshaler
	sink        *mockSink
	tx          *mockTransactor
	saver       *EntitySaver
}

func TestEntitySaverSuite(t *testing.T) { suite.Run(t, new(EntitySaverSuite)) }

func (s *EntitySaverSuite) SetupTest() {
	s.ctx = context.Background()
	s.entityRepo = &mockEntityRepository{}
	s.entityMarsh = &mockEntityMarshaler[*fakeEntity]{}
	s.eventRepo = &mockEventRepository{}
	s.eventMarsh = &mockEventMarshaler{}
	s.sink = &mockSink{}
	s.tx = &mockTransactor{}
	publisher := AtLeastOnce(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarsh)
	s.saver = NewEntitySaver(publisher, s.tx, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))
}

func (s *EntitySaverSuite) TearDownTest() {
	s.entityRepo.AssertExpectations(s.T())
	s.entityMarsh.AssertExpectations(s.T())
	s.eventRepo.AssertExpectations(s.T())
	s.eventMarsh.AssertExpectations(s.T())
	s.sink.AssertExpectations(s.T())
	s.tx.AssertExpectations(s.T())
}
```

In the same file, replace the `events := NewEventStore(...)` / `saver := NewEntitySaver(events, ...)` pair inside `TestSaveTwoTypesOneTx` (lines 143-147) with:

```go
	publisher := AtLeastOnce(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarsh)
	saver := NewEntitySaver(publisher, s.tx,
		Bind[*fakeEntity](s.entityRepo, s.entityMarsh),
		Bind[*fakeEntity2](repo2, marsh2),
	)
```

and delete the now-stale `events := ...` line above it.

Append these two tests to `saver_test.go`:

```go
func (s *EntitySaverSuite) TestBestEffortDeliversAfterCommit() {
	tx := &recordingTransactor{}
	publisher := BestEffort(stubIDer{id: "evt-1"}, s.sink, NoopMetadataGetter{}, s.eventMarsh)
	saver := NewEntitySaver(publisher, tx, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(nil).Once().
		Run(func(mock.Arguments) {
			s.True(tx.committed, "delivery must run after commit")
		})

	err := saver.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Empty(e.events().All())
	s.Equal(uint64(1), e.Version().Value())
	// eventRepo is never touched under BestEffort — asserted by TearDownTest.
}

func (s *EntitySaverSuite) TestBestEffortDeliveryFailureStillAdvancesEntity() {
	tx := &recordingTransactor{}
	publisher := BestEffort(stubIDer{id: "evt-1"}, s.sink, NoopMetadataGetter{}, s.eventMarsh)
	saver := NewEntitySaver(publisher, tx, Bind[*fakeEntity](s.entityRepo, s.entityMarsh))

	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.entityMarsh.On("Marshal", mock.Anything, e).Return(m, nil)
	s.entityRepo.On("Save", mock.Anything, m).Return(nil)
	s.eventMarsh.On("Marshal", mock.Anything, mock.Anything).
		Return(&MarshaledEvent{Type: "Created", Data: []byte(`{}`)}, nil)
	s.sink.On("Publish", mock.Anything, mock.Anything).Return(errors.New("broker down")).Once()

	err := saver.Save(s.ctx, e)

	s.Require().ErrorIs(err, ErrDeliveryFailed)
	// State committed, so the entity must match durable state regardless.
	s.Equal(uint64(1), e.Version().Value())
	s.Empty(e.events().All())
}
```

In `store_test.go`, replace lines 31-32 with:

```go
	publisher := AtLeastOnce(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarsh)
	s.store = NewEntityStore[*fakeEntity](s.repo, s.marshaler, publisher, s.tx)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test . -run 'TestEntitySaverSuite|TestEntityStoreSuite' -v`
Expected: FAIL — `NewEntitySaver` still wants a `*EventStore`, so the compile fails with `cannot use publisher (variable of type *Publisher) as *EventStore value`.

- [ ] **Step 4: Point EntitySaver at Publisher**

In `saver.go`, replace the struct and constructor (lines 13-27):

```go
// EntitySaver
type EntitySaver struct {
	bindings  map[string]binding
	publisher *Publisher
	tx        Transactor
}

func NewEntitySaver(p *Publisher, tx Transactor, bindings ...binder) *EntitySaver {
	m := make(map[string]binding, len(bindings))
	for _, b := range bindings {
		bd := b.binding()
		m[bd.typ] = bd
	}
	return &EntitySaver{bindings: m, publisher: p, tx: tx}
}
```

Replace `Save` (lines 29-78):

```go
func (s *EntitySaver) Save(ctx context.Context, es ...Entity) error {
	if len(es) == 0 {
		return nil
	}

	var events []Event
	for _, e := range es {
		events = append(events, e.events().All()...)
	}

	type entities struct {
		e Entity
		v Version
	}

	var (
		saved   []entities
		deliver delivery
	)

	fn := func(ctx context.Context) error {
		saved = nil
		deliver = nil

		for _, e := range es {
			v, err := s.save(ctx, e)
			if err != nil {
				return err
			}

			saved = append(saved, entities{e: e, v: v})
		}

		var err error
		deliver, err = s.publisher.stage(ctx, events...)
		return err
	}

	var err error
	if len(es) == 1 && len(events) == 0 {
		err = fn(ctx)
	} else {
		err = s.tx.WithinTx(ctx, fn)
	}

	if err != nil {
		return err
	}

	for _, p := range saved {
		p.e.SetVersion(p.v)
		p.e.events().Clear()
	}

	if deliver == nil {
		return nil
	}
	if err := deliver(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("%w: %w", ErrDeliveryFailed, err)
	}
	return nil
}
```

`saver.go` already imports `context`, `errors`, and `fmt` — no import changes needed.

- [ ] **Step 5: Update EntityStore**

In `store.go`, replace the constructor (lines 11-18):

```go
func NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E], p *Publisher, tx Transactor) *EntityStore[E] {
	b := Bind[E](r, m)

	return &EntityStore[E]{
		loader: NewEntityLoader(b),
		saver:  NewEntitySaver(p, tx, b),
	}
}
```

- [ ] **Step 6: Delete EventStore**

In `event.go`, delete lines 87-110 — the `EventStore` type, `NewEventStore`, and its `Save` method. Keep `envelopeBuilder`, `build`, and everything above it. Update the `envelopeBuilder` doc comment on line 57 from `// envelopeBuilder stamps events into envelopes; shared by EventStore and Publisher.` to:

```go
// envelopeBuilder stamps events into envelopes.
```

Then delete the obsolete test file, whose coverage now lives in `publisher_test.go`:

```bash
git rm event_test.go
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `go test . -v`
Expected: PASS, including `TestEntitySaverSuite` with the two new BestEffort tests and `TestEntityStoreSuite`.

- [ ] **Step 8: Run the full build and suite**

Run: `go build ./... && go test ./...`
Expected: PASS across all packages.

- [ ] **Step 9: Verify no stale references remain**

Run: `grep -rn "Notifier\|EventStore\|ext\.Transport\|ember\.Transport\|noop\." --include="*.go" .`
Expected: no output. If `postgres/event_repository.go:14` still mentions `ember.EventStore`, update that comment to say `the Publisher (inside a transaction)`.

- [ ] **Step 10: Commit**

```bash
git add saver.go store.go event.go mocks_test.go saver_test.go store_test.go event_test.go postgres/event_repository.go
git commit -m "refactor: point EntitySaver at Publisher and delete EventStore"
```

---

## Not in this plan

- **Event ordering redesign** (`seq = Timestamp.UnixNano()` → per-entity `(version, intra-save index)`). The relay still drains `ORDER BY seq`. The postgres relay must not be wired until that lands.
- **`sparkmw.Atomic` retirement and per-service UoW adoption.** Separate spec. Dropping `Atomic` before a service moves to `Emit` + `EntitySaver` would regress it from atomic to a genuine dual-write.
- **Relay nudge / `Waker`.** Deferred; adds later with no API break by having `atLeastOnce.stage` return a delivery that wakes the relay.
- **Consuming service migration.** All nine `main.go` files need rewiring from `ember.NewPublisher` + `embermongo.NewNotifier` to `ember.AtLeastOnce` + `ember.NewRelay`, plus an ember version bump. Out of scope here.
