# Ember Unit of Work Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a single ember entity and the domain events it produced commit atomically, with no dependency on spark.

**Architecture:** Entities buffer their domain events on `EntityRoot` (via a new `Events` type, drained through a sealed `events()` seam on the `Entity` interface). A persist-only `EventStore` mirrors `EntityStore`. A new `Store[E]` composes `EntityStore` + `EventStore` + a reentrant `Transactor` and persists entity + events in one transaction; the existing outbox relay delivers post-commit. No notifier is reachable from the transactional path, so mid-tx delivery ("phantom events") is structurally impossible.

**Tech Stack:** Go, `github.com/klemen-forstneric/ember`, mongo-driver v2 (v2.6.0), testify (`suite` + `mock`).

## Global Constraints

- Module: `github.com/klemen-forstneric/ember`. All paths below are relative to the `ember/` repo root.
- Tests are white-box: `package ember` (not `ember_test`), matching existing `entity_test.go`.
- Test style: testify `suite.Suite` with `SetupTest`/`TearDownTest`; test doubles are `testify/mock` types kept unexported in `mocks_test.go`.
- No new external dependencies.
- Comments: terse, only where non-obvious. No multi-line rationale blocks.
- This slice is **ember-only and non-breaking**. Do NOT touch `Publisher`'s constructor signature, `mongo.Notifier`, or any consuming service. The publisher/relay rename is a deferred follow-up (see spec).
- Spec: `docs/superpowers/specs/2026-07-24-ember-uow-design.md`.

## File Structure

- `events.go` (create) — the `Events` buffer type. One responsibility: accumulate/read/clear an entity's pending events.
- `entity.go` (modify) — `EntityRoot` gains the buffer + `Emit`/`events()`; `Entity` interface gains sealed `events()`; `EntityStore.Save`/`save` split + `ErrUnpublishedEvents` guard.
- `event.go` (modify) — `envelopeBuilder` (shared) + persist-only `EventStore`.
- `publisher.go` (modify) — refactor to reuse `envelopeBuilder`; external behavior unchanged.
- `store.go` (create) — `Store[E]`, the atomic unit of work.
- `transactor.go` (create) — `Transactor` interface.
- `mongo/transactor.go` (create) — reentrant mongo `Transactor`.
- `mocks_test.go` (modify) — add event-side mocks reused by `EventStore` and `Store` tests.

---

### Task 1: `Events` buffer type

**Files:**
- Create: `events.go`
- Test: `events_test.go`

**Interfaces:**
- Consumes: existing `Event` interface (`EntityID() string`, `Type() string`) from `event.go`.
- Produces: `type Events []Event` with methods `Emit(events ...Event)`, `All() []Event`, `Clear()` — all pointer-receiver.

- [ ] **Step 1: Write the failing test**

Create `events_test.go`:

```go
package ember

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeEvent is minimal test data implementing Event.
type fakeEvent struct {
	entityID string
	typ      string
}

func (e fakeEvent) EntityID() string { return e.entityID }
func (e fakeEvent) Type() string     { return e.typ }

func TestEventsEmitAndAll(t *testing.T) {
	var buf Events
	a := fakeEvent{entityID: "1", typ: "A"}
	b := fakeEvent{entityID: "1", typ: "B"}

	buf.Emit(a)
	buf.Emit(b)

	require.Equal(t, []Event{a, b}, buf.All())
}

func TestEventsAllClonesBuffer(t *testing.T) {
	var buf Events
	buf.Emit(fakeEvent{entityID: "1", typ: "A"})

	got := buf.All()
	got[0] = fakeEvent{entityID: "x", typ: "X"} // mutate the returned slice

	require.Equal(t, "A", buf.All()[0].Type()) // buffer unaffected
}

func TestEventsClearThenEmit(t *testing.T) {
	var buf Events
	buf.Emit(fakeEvent{entityID: "1", typ: "A"})
	buf.Clear()
	require.Empty(t, buf.All())

	buf.Emit(fakeEvent{entityID: "1", typ: "B"}) // append after nil must not panic
	require.Len(t, buf.All(), 1)
	require.Equal(t, "B", buf.All()[0].Type())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ -run 'TestEvents' -v`
Expected: FAIL — `undefined: Events`.

- [ ] **Step 3: Write minimal implementation**

Create `events.go`:

```go
package ember

import "slices"

// Events is a transient buffer of the domain events an entity has produced. It
// is never serialized; the store drains it when the entity is persisted.
type Events []Event

func (e *Events) Emit(events ...Event) { *e = append(*e, events...) }
func (e *Events) All() []Event         { return slices.Clone(*e) }
func (e *Events) Clear()               { *e = nil }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./ -run 'TestEvents' -v`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add events.go events_test.go
git commit -m "feat(ember): add Events domain-event buffer

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `EntityRoot` buffer + sealed `events()` on `Entity`

**Files:**
- Modify: `entity.go` (the `Entity` interface and `EntityRoot` block)
- Test: `entity_test.go`

**Interfaces:**
- Consumes: `Events` from Task 1; existing `Version`, `NewVersion`.
- Produces:
  - `Entity` interface gains unexported `events() *Events`.
  - `EntityRoot` fields renamed to `i string`, `v Version`, add `e Events`.
  - `EntityRoot` methods: `ID`/`Version`/`SetVersion` (unchanged behavior), new `Emit(events ...Event)`, new `events() *Events`.

- [ ] **Step 1: Write the failing test**

Append to `entity_test.go`:

```go
func TestEntityRootEmitBuffersEvents(t *testing.T) {
	e := newFakeEntity("1")
	evt := fakeEvent{entityID: "1", typ: "Created"}

	e.Emit(evt)

	require.Equal(t, []Event{evt}, e.events().All())
}

func TestEntityRootIdentityUnchanged(t *testing.T) {
	e := newFakeEntity("42")
	require.Equal(t, "42", e.ID())
	require.Equal(t, uint64(0), e.Version().Value())

	e.SetVersion(NewVersion(3))
	require.Equal(t, uint64(3), e.Version().Value())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ -run 'TestEntityRoot' -v`
Expected: FAIL — `e.Emit undefined` / `e.events undefined`.

- [ ] **Step 3: Write minimal implementation**

In `entity.go`, replace the `Entity` interface and the `EntityRoot` block.

Interface — add the sealed seam:

```go
type Entity interface {
	ID() string
	Type() string
	Version() Version
	SetVersion(Version)
	events() *Events // unexported: only ember-rooted types satisfy Entity
}
```

`EntityRoot` — rename fields, add buffer + methods:

```go
// EntityRoot supplies identity, optimistic-concurrency version, and a domain-
// event buffer to an entity. None of these are serialized here — persistence is
// owned by a per-entity marshaler.
type EntityRoot struct {
	i string
	v Version
	e Events
}

func NewEntityRoot(id string) EntityRoot {
	return EntityRoot{i: id, v: NewVersion(0)}
}

func (r *EntityRoot) ID() string           { return r.i }
func (r *EntityRoot) Version() Version      { return r.v }
func (r *EntityRoot) SetVersion(v Version)  { r.v = v }
func (r *EntityRoot) Emit(events ...Event)  { r.e.Emit(events...) }
func (r *EntityRoot) events() *Events       { return &r.e }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./ -run 'TestEntityRoot' -v`
Expected: PASS.

- [ ] **Step 5: Verify the whole package still compiles and passes**

Run: `go build ./... && go test ./`
Expected: PASS. (`fakeEntity` embeds `EntityRoot`, so it satisfies the extended `Entity` interface automatically.)

- [ ] **Step 6: Commit**

```bash
git add entity.go entity_test.go
git commit -m "feat(ember): buffer domain events on EntityRoot via sealed events()

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Split `EntityStore.save`/`Save` + `ErrUnpublishedEvents` guard

**Files:**
- Modify: `entity.go` (the `EntityStore.Save` method + error vars)
- Test: `entity_test.go`

**Interfaces:**
- Consumes: `EntityRoot.events()` from Task 2.
- Produces:
  - `var ErrUnpublishedEvents error`.
  - `EntityStore.save(ctx, E) error` — unexported, the existing snapshot logic, no event awareness. Used by `Store` (Task 6).
  - `EntityStore.Save(ctx, E) error` — public; rejects entities with pending events, else delegates to `save`.

- [ ] **Step 1: Write the failing test**

Append to `entity_test.go`:

```go
func (s *EntityStoreSuite) TestSaveRejectsEntityWithPendingEvents() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})

	err := s.store.Save(s.ctx, e)

	s.ErrorIs(err, ErrUnpublishedEvents)
	// repo.Marshal / repo.Save must NOT be called — asserted by TearDownTest
	// (no expectations set on the mocks).
}

func (s *EntityStoreSuite) TestSaveWithoutEventsPersists() {
	e := newFakeEntity("1")
	m := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.marshaler.On("Marshal", mock.Anything, e).Return(m, nil)
	s.repo.On("Save", mock.Anything, m).Return(nil)

	err := s.store.Save(s.ctx, e)

	s.Require().NoError(err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./ -run 'TestEntityStoreSuite' -v`
Expected: FAIL — `undefined: ErrUnpublishedEvents` (and `TestSaveRejects...` would otherwise call the old Save).

- [ ] **Step 3: Write minimal implementation**

In `entity.go`, add the error var beside the existing ones:

```go
var (
	ErrEntityNotFound   = errors.New("ember: entity not found")
	ErrVersionConflict  = errors.New("ember: entity version conflict")
	ErrUnpublishedEvents = errors.New("ember: entity has pending events; use ember.Store")
)
```

Replace the existing `EntityStore.Save` with the guarded public method plus the unexported `save` (the body is the current `Save` body verbatim):

```go
// Save persists an entity snapshot. A snapshot-only store must not silently drop
// domain events: an entity with pending events must be saved through ember.Store.
func (s *EntityStore[E]) Save(ctx context.Context, e E) error {
	if len(e.events().All()) > 0 {
		return ErrUnpublishedEvents
	}
	return s.save(ctx, e)
}

func (s *EntityStore[E]) save(ctx context.Context, e E) error {
	next := e.Version().Inc()
	e.SetVersion(next)

	m, err := s.marshaler.Marshal(ctx, e)
	if err != nil {
		return err
	}

	if err := s.repository.Save(ctx, m); err != nil {
		return err
	}

	// Collapse the version so a subsequent Save of the same in-memory entity
	// filters on the just-persisted version rather than the original Initial().
	e.SetVersion(NewVersion(next.Value()))
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./ -run 'TestEntityStoreSuite' -v`
Expected: PASS (including the pre-existing suite tests and `TestEntityStoreRepeatedSaveOfSameInstance`).

- [ ] **Step 5: Commit**

```bash
git add entity.go entity_test.go
git commit -m "feat(ember): guard EntityStore.Save against dropping pending events

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: `envelopeBuilder` + persist-only `EventStore` (+ Publisher refactor)

**Files:**
- Modify: `event.go` (add `envelopeBuilder`, `EventStore`, `NewEventStore`)
- Modify: `publisher.go` (reuse `envelopeBuilder`; behavior unchanged)
- Modify: `mocks_test.go` (add `mockEventRepository`, `mockEventMarshaler`, `stubIDer`)
- Test: `event_test.go` (create)

**Interfaces:**
- Consumes: existing `IDer` (`ID() string`), `EventRepository` (`Save(ctx, []EventEnvelope) error`), `MetadataGetter` (`Get(ctx) (Metadata, error)`), `EventMarshaler` (`Marshal(ctx, Event) (*MarshaledEvent, error)`), `EventEnvelope`.
- Produces:
  - `EventStore` struct + `NewEventStore(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler) *EventStore`.
  - `EventStore.Save(ctx context.Context, events ...Event) error`.
  - unexported `envelopeBuilder{ ider IDer; metadata MetadataGetter; marshaler EventMarshaler }` with `build(ctx, events ...Event) ([]EventEnvelope, error)`.

- [ ] **Step 1: Add the event-side test mocks**

Append to `mocks_test.go`:

```go
// mockEventRepository is a testify mock for EventRepository.
type mockEventRepository struct {
	mock.Mock
}

func (m *mockEventRepository) Save(ctx context.Context, envelopes []EventEnvelope) error {
	return m.Called(ctx, envelopes).Error(0)
}

// mockEventMarshaler is a testify mock for EventMarshaler.
type mockEventMarshaler struct {
	mock.Mock
}

func (m *mockEventMarshaler) Marshal(ctx context.Context, e Event) (*MarshaledEvent, error) {
	args := m.Called(ctx, e)
	var out *MarshaledEvent
	if v := args.Get(0); v != nil {
		out = v.(*MarshaledEvent)
	}
	return out, args.Error(1)
}

func (m *mockEventMarshaler) Unmarshal(ctx context.Context, e *MarshaledEvent) (Event, error) {
	args := m.Called(ctx, e)
	var out Event
	if v := args.Get(0); v != nil {
		out = v.(Event)
	}
	return out, args.Error(1)
}

// stubIDer returns a fixed id (event envelope IDs are not under test here).
type stubIDer struct{ id string }

func (s stubIDer) ID() string { return s.id }
```

- [ ] **Step 2: Write the failing test**

Create `event_test.go`:

```go
package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type EventStoreSuite struct {
	suite.Suite
	ctx       context.Context
	repo      *mockEventRepository
	marshaler *mockEventMarshaler
	store     *EventStore
}

func TestEventStoreSuite(t *testing.T) {
	suite.Run(t, new(EventStoreSuite))
}

func (s *EventStoreSuite) SetupTest() {
	s.ctx = context.Background()
	s.repo = &mockEventRepository{}
	s.marshaler = &mockEventMarshaler{}
	s.store = NewEventStore(stubIDer{id: "evt-1"}, s.repo, NoopMetadataGetter{}, s.marshaler)
}

func (s *EventStoreSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
	s.marshaler.AssertExpectations(s.T())
}

func (s *EventStoreSuite) TestSavePersistsEnvelopes() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	marshaled := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(marshaled, nil)
	s.repo.On("Save", mock.Anything, mock.MatchedBy(func(envs []EventEnvelope) bool {
		return len(envs) == 1 &&
			envs[0].ID == "evt-1" &&
			envs[0].EntityID == "A" &&
			envs[0].Event == marshaled
	})).Return(nil)

	err := s.store.Save(s.ctx, evt)

	s.Require().NoError(err)
}

func (s *EventStoreSuite) TestSaveMarshalError() {
	evt := fakeEvent{entityID: "A", typ: "Created"}
	s.marshaler.On("Marshal", mock.Anything, evt).Return(nil, errors.New("boom"))

	err := s.store.Save(s.ctx, evt)

	s.Require().Error(err)
	// repo.Save must NOT be called — asserted by TearDownTest.
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./ -run 'TestEventStoreSuite' -v`
Expected: FAIL — `undefined: NewEventStore`.

- [ ] **Step 4: Write minimal implementation**

In `event.go`, add (keep the existing types; add these after `EventEnvelope`/`Event`):

```go
// envelopeBuilder turns domain events into stamped envelopes. Shared by
// EventStore (persist) and Publisher (immediate delivery).
type envelopeBuilder struct {
	ider      IDer
	metadata  MetadataGetter
	marshaler EventMarshaler
}

func (b envelopeBuilder) build(ctx context.Context, events ...Event) ([]EventEnvelope, error) {
	metadata, err := b.metadata.Get(ctx)
	if err != nil {
		return nil, err
	}

	envelopes := make([]EventEnvelope, 0, len(events))
	for _, e := range events {
		marshaled, err := b.marshaler.Marshal(ctx, e)
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, EventEnvelope{
			ID:        b.ider.ID(),
			EntityID:  e.EntityID(),
			Event:     marshaled,
			Metadata:  metadata,
			Timestamp: time.Now().UTC(),
		})
	}
	return envelopes, nil
}

// EventStore is to events what EntityStore is to entities: it builds the envelope
// and persists it to the EventRepository. It never delivers — the outbox relay
// polls and delivers post-commit.
type EventStore struct {
	builder    envelopeBuilder
	repository EventRepository
}

func NewEventStore(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler) *EventStore {
	return &EventStore{
		builder:    envelopeBuilder{ider: i, metadata: mg, marshaler: m},
		repository: r,
	}
}

func (s *EventStore) Save(ctx context.Context, events ...Event) error {
	envelopes, err := s.builder.build(ctx, events...)
	if err != nil {
		return err
	}
	return s.repository.Save(ctx, envelopes)
}
```

Add the `time` import to `event.go` if not already present (it is used by the existing `EventEnvelope`/`ReceivedEvent` types, so the import already exists — verify).

- [ ] **Step 5: Refactor `Publisher` to reuse `envelopeBuilder` (no behavior change)**

Replace the body of `publisher.go` with:

```go
package ember

import "context"

// IDer
type IDer interface {
	ID() string
}

// Publisher
type Publisher struct {
	builder    envelopeBuilder
	repository EventRepository
	notifier   Notifier
}

func NewPublisher(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler, n Notifier) *Publisher {
	return &Publisher{
		builder:    envelopeBuilder{ider: i, metadata: mg, marshaler: m},
		repository: r,
		notifier:   n,
	}
}

func (p *Publisher) Publish(ctx context.Context, events ...Event) error {
	envelopes, err := p.builder.build(ctx, events...)
	if err != nil {
		return err
	}
	if err := p.repository.Save(ctx, envelopes); err != nil {
		return err
	}
	p.notifier.Notify(ctx, envelopes)
	return nil
}
```

(The `time` import moves out of `publisher.go` — it now lives with `envelopeBuilder` in `event.go`. `IDer` is unchanged; it stays declared here.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `go build ./... && go test ./ -run 'TestEventStoreSuite' -v && go test ./`
Expected: PASS. The `Publisher` refactor preserves behavior; any existing publisher tests stay green.

- [ ] **Step 7: Commit**

```bash
git add event.go publisher.go mocks_test.go event_test.go
git commit -m "feat(ember): add persist-only EventStore; share envelope builder

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: `Transactor` interface + reentrant mongo implementation

**Files:**
- Create: `transactor.go`
- Create: `mongo/transactor.go`
- Test: `mongo/transactor_test.go` (create)

**Interfaces:**
- Produces:
  - `ember.Transactor` interface: `WithinTx(ctx context.Context, fn func(ctx context.Context) error) error`.
  - `mongo.Transactor` struct + `NewTransactor(client *mongo.Client) *Transactor`, satisfying `ember.Transactor` structurally.

- [ ] **Step 1: Create the interface**

Create `transactor.go`:

```go
package ember

import "context"

// Transactor runs fn inside a transaction, passing a ctx bound to it. Reentrant
// implementations join a transaction already present on the ctx rather than
// nesting a new one.
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
```

- [ ] **Step 2: Write the failing test (reentrant-join branch)**

Create `mongo/transactor_test.go`:

```go
package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type TransactorSuite struct {
	suite.Suite
	client *mongo.Client
	tx     *Transactor
}

func TestTransactorSuite(t *testing.T) {
	suite.Run(t, new(TransactorSuite))
}

func (s *TransactorSuite) SetupTest() {
	// connectTestMongo (sort_test.go, same package) skips when mongo is
	// unavailable; reuse its client. StartSession needs no replica set.
	s.client = connectTestMongo(s.T()).Database().Client()
	s.tx = NewTransactor(s.client)
}

// When the ctx already carries a session, WithinTx joins it: it runs fn on the
// same session-bound ctx and does not start a second session/transaction.
func (s *TransactorSuite) TestWithinTxJoinsExistingSession() {
	sess, err := s.client.StartSession()
	s.Require().NoError(err)
	defer sess.EndSession(context.Background())

	sctx := mongo.NewSessionContext(context.Background(), sess)

	called := false
	err = s.tx.WithinTx(sctx, func(ctx context.Context) error {
		called = true
		s.NotNil(mongo.SessionFromContext(ctx), "fn must receive a session-bound ctx")
		return nil
	})

	s.Require().NoError(err)
	s.True(called)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./mongo/ -run 'TestTransactorSuite' -v`
Expected: FAIL — `undefined: NewTransactor` (or SKIP if mongo is unavailable; if skipped, that is acceptable — the implementation step still applies).

- [ ] **Step 4: Write minimal implementation**

Create `mongo/transactor.go`:

```go
package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Transactor runs work inside a mongo transaction. Reentrant: if the ctx already
// carries a session (an outer WithinTx, or another framework's transaction such
// as spark's Atomic middleware), it joins that transaction instead of starting a
// nested one, which mongo forbids.
type Transactor struct {
	client *mongo.Client
}

func NewTransactor(client *mongo.Client) *Transactor {
	return &Transactor{client: client}
}

func (t *Transactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	if mongo.SessionFromContext(ctx) != nil {
		return fn(ctx)
	}

	sess, err := t.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(txCtx context.Context) (any, error) {
		return nil, fn(txCtx)
	})
	return err
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./mongo/ -run 'TestTransactorSuite' -v`
Expected: PASS (or SKIP when mongo is unavailable).

Note: the own-transaction branch (`StartSession` + `WithTransaction`) requires a mongo replica set and is exercised end-to-end by services; the unit test above covers the reentrant decision that is new here.

- [ ] **Step 6: Commit**

```bash
git add transactor.go mongo/transactor.go mongo/transactor_test.go
git commit -m "feat(ember): add reentrant mongo Transactor

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: `Store[E]` — the atomic unit of work

**Files:**
- Create: `store.go`
- Test: `store_test.go` (create)
- Modify: `mocks_test.go` (add `mockTransactor`)

**Interfaces:**
- Consumes: `EntityStore[E]` + its `save`/`Get`/`List` (Tasks 3), `EventStore.Save` (Task 4), `Transactor` (Task 5), `EntityRoot.events()` (Task 2).
- Produces:
  - `Store[E Entity]` struct + `NewStore[E Entity](es *EntityStore[E], ev *EventStore, tx Transactor) *Store[E]`.
  - `Store.Get(ctx, id) (E, error)`, `Store.List(ctx, Filter, Sort) ([]E, error)`, `Store.Save(ctx, E) error`.

- [ ] **Step 1: Add the transactor mock**

Append to `mocks_test.go`:

```go
// mockTransactor runs the callback (as a real transaction boundary does) and
// returns the handler's error, else the configured transaction-level error.
type mockTransactor struct {
	mock.Mock
}

func (m *mockTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	args := m.Called(ctx)
	if err := fn(ctx); err != nil {
		return err
	}
	return args.Error(0)
}
```

- [ ] **Step 2: Write the failing test**

Create `store_test.go`. It wires a real `EntityStore` + real `EventStore` over mock repositories, plus the `mockTransactor`, so the orchestration is exercised end-to-end:

```go
package ember

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type StoreSuite struct {
	suite.Suite
	ctx            context.Context
	entityRepo     *mockEntityRepository
	entityMarshal  *mockEntityMarshaler[*fakeEntity]
	eventRepo      *mockEventRepository
	eventMarshal   *mockEventMarshaler
	tx             *mockTransactor
	store          *Store[*fakeEntity]
}

func TestStoreSuite(t *testing.T) {
	suite.Run(t, new(StoreSuite))
}

func (s *StoreSuite) SetupTest() {
	s.ctx = context.Background()
	s.entityRepo = &mockEntityRepository{}
	s.entityMarshal = &mockEntityMarshaler[*fakeEntity]{}
	s.eventRepo = &mockEventRepository{}
	s.eventMarshal = &mockEventMarshaler{}
	s.tx = &mockTransactor{}

	entities := NewEntityStore[*fakeEntity](s.entityRepo, s.entityMarshal)
	events := NewEventStore(stubIDer{id: "evt-1"}, s.eventRepo, NoopMetadataGetter{}, s.eventMarshal)
	s.store = NewStore[*fakeEntity](entities, events, s.tx)
}

func (s *StoreSuite) TearDownTest() {
	s.entityRepo.AssertExpectations(s.T())
	s.entityMarshal.AssertExpectations(s.T())
	s.eventRepo.AssertExpectations(s.T())
	s.eventMarshal.AssertExpectations(s.T())
	s.tx.AssertExpectations(s.T())
}

func (s *StoreSuite) TestSavePersistsEntityAndEventsThenClears() {
	e := newFakeEntity("1")
	evt := fakeEvent{entityID: "1", typ: "Created"}
	e.Emit(evt)

	me := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	mev := &MarshaledEvent{Type: "Created", Data: []byte(`{}`)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarshal.On("Marshal", mock.Anything, e).Return(me, nil)
	s.entityRepo.On("Save", mock.Anything, me).Return(nil)
	s.eventMarshal.On("Marshal", mock.Anything, evt).Return(mev, nil)
	s.eventRepo.On("Save", mock.Anything, mock.Anything).Return(nil)

	err := s.store.Save(s.ctx, e)

	s.Require().NoError(err)
	s.Empty(e.events().All(), "buffer cleared after commit")
}

func (s *StoreSuite) TestSaveWithNoEventsSkipsEventStore() {
	e := newFakeEntity("1")
	me := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarshal.On("Marshal", mock.Anything, e).Return(me, nil)
	s.entityRepo.On("Save", mock.Anything, me).Return(nil)

	err := s.store.Save(s.ctx, e)

	s.Require().NoError(err)
	// eventRepo.Save / eventMarshal.Marshal not called — asserted by TearDownTest.
}

func (s *StoreSuite) TestSaveEntityFailureSkipsEventsAndKeepsBuffer() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarshal.On("Marshal", mock.Anything, e).Return(nil, errors.New("marshal boom"))

	err := s.store.Save(s.ctx, e)

	s.Require().Error(err)
	s.Len(e.events().All(), 1, "buffer intact on failure for retry")
	// eventRepo untouched — asserted by TearDownTest.
}

func (s *StoreSuite) TestSaveEventFailureKeepsBuffer() {
	e := newFakeEntity("1")
	e.Emit(fakeEvent{entityID: "1", typ: "Created"})
	me := &MarshaledEntity{ID: "1", Type: "fake", Version: NewVersion(1)}
	s.tx.On("WithinTx", mock.Anything).Return(nil).Once()
	s.entityMarshal.On("Marshal", mock.Anything, e).Return(me, nil)
	s.entityRepo.On("Save", mock.Anything, me).Return(nil)
	s.eventMarshal.On("Marshal", mock.Anything, mock.Anything).Return(nil, errors.New("event boom"))

	err := s.store.Save(s.ctx, e)

	s.Require().Error(err)
	s.Len(e.events().All(), 1, "buffer intact on failure for retry")
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./ -run 'TestStoreSuite' -v`
Expected: FAIL — `undefined: Store` / `undefined: NewStore`.

- [ ] **Step 4: Write minimal implementation**

Create `store.go`:

```go
package ember

import "context"

// Store is a unit of work: it persists an entity and the domain events it
// produced in one transaction. Delivery is the outbox relay's job — Store holds
// no notifier, so it cannot deliver mid-transaction.
type Store[E Entity] struct {
	entities *EntityStore[E]
	events   *EventStore
	tx       Transactor
}

func NewStore[E Entity](es *EntityStore[E], ev *EventStore, tx Transactor) *Store[E] {
	return &Store[E]{entities: es, events: ev, tx: tx}
}

func (s *Store[E]) Get(ctx context.Context, id string) (E, error) {
	return s.entities.Get(ctx, id)
}

func (s *Store[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
	return s.entities.List(ctx, f, sort)
}

func (s *Store[E]) Save(ctx context.Context, e E) error {
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := s.entities.save(ctx, e); err != nil {
			return err
		}
		evs := e.events().All()
		if len(evs) == 0 {
			return nil
		}
		return s.events.Save(ctx, evs...)
	})
	if err != nil {
		return err
	}
	e.events().Clear()
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./ -run 'TestStoreSuite' -v`
Expected: PASS (all four cases).

- [ ] **Step 6: Full suite + build**

Run: `go build ./... && go test ./...`
Expected: PASS (mongo tests may SKIP when mongo is unavailable).

- [ ] **Step 7: Commit**

```bash
git add store.go store_test.go mocks_test.go
git commit -m "feat(ember): add Store unit of work (atomic entity save + event persist)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- §1 Events buffer → Task 1. ✓
- §2 sealed `events()` on `Entity`, `EntityRoot` buffer/`Emit` → Task 2. ✓
- §3 reentrant `Transactor` → Task 5. ✓
- §4 persist-only `EventStore` + shared envelope builder → Task 4. ✓
- §5 `Store[E]` atomic unit, no notify, clear-on-commit → Task 6. ✓
- §6 `save`/`Save` split + `ErrUnpublishedEvents` guard → Task 3. ✓
- Data flow / error handling (rollback keeps buffer; guard) → Tasks 3 & 6 tests. ✓
- Files list matches (`Store[E]` placed in its own `store.go` for focus, rather than `entity.go` as the spec's file list noted — a focused-file improvement, same behavior). ✓
- Non-goal: no `Publisher` constructor change, no `mongo.Notifier` change, no service edits → honored; follow-up left out. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; commands have expected output. ✓

**Type consistency:**
- `events() *Events`, `Emit`, `All`, `Clear` consistent across Tasks 1/2/3/6. ✓
- `EntityStore.save` (unexported) defined in Task 3, consumed in Task 6. ✓
- `NewEventStore(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler)` and `EventStore.Save(ctx, ...Event)` consistent Tasks 4/6. ✓
- `NewStore[E](es *EntityStore[E], ev *EventStore, tx Transactor)` consistent Task 6. ✓
- `Transactor.WithinTx` signature identical in `transactor.go`, mongo impl, and `mockTransactor`. ✓
- `stubIDer`, `mockEventRepository`, `mockEventMarshaler` defined in Task 4, reused in Task 6. ✓
