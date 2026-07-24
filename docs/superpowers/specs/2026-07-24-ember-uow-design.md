# Ember Unit of Work: atomic entity save + event publish

**Date:** 2026-07-24
**Scope:** `ember` library only. No service refactors in this slice.

## Problem

Ember persists an entity snapshot (`EntityStore.Save`) and publishes domain
events to the outbox (`Publisher.Publish`) as two independent calls. Their
atomicity is implicit: it holds only when something opens a transaction on the
`ctx` first. Today that "something" is spark's `Atomic` middleware, so save +
publish are atomic **only** for services driving ember through the spark command
bus. Call ember outside a command handler and a failed publish can leave a
persisted entity with no event — an inconsistent outbox.

Goal: give ember its own unit-of-work so a single entity and the events it
produced commit atomically, with no dependency on spark, and with near-invisible
developer ergonomics.

## Non-goals

- No `Loader`/`Saver` (typed-read / heterogeneous-write) split yet.
- No multi-entity-per-unit batching yet (one entity + its events).
- No event sourcing: state is still mutated imperatively and persisted as a
  snapshot; `Get` reads the snapshot, never replays events.
- No changes to the outbox relay (`ember/mongo` notifier) — it already delivers
  post-commit by polling.
- No adoption in the 8 consuming services. That is a later, separate step.

## Design

### 1. Domain events buffered on the aggregate

A new `Events` type owns all buffer mechanics:

```go
// events.go
type Events []Event

func (e *Events) Emit(events ...Event) { *e = append(*e, events...) }
func (e *Events) All() []Event         { return slices.Clone(*e) } // non-destructive
func (e *Events) Clear()               { *e = nil }
```

`EntityRoot` holds one, with short receiver-scoped field names so the accessor
method can be named `events`:

```go
type EntityRoot struct {
    i string
    v Version
    e Events
}

func (r *EntityRoot) ID() string          { return r.i }
func (r *EntityRoot) Version() Version     { return r.v }
func (r *EntityRoot) SetVersion(v Version) { r.v = v }
func (r *EntityRoot) Emit(events ...Event) { r.e.Emit(events...) } // domain write
func (r *EntityRoot) events() *Events      { return &r.e }         // sealed seam
```

`Type()` is not defined on `EntityRoot` — each concrete entity supplies its own
(e.g. `func (o *Order) Type() string { return "order" }`), unchanged by this work.

The aggregate's own behavior records events alongside the state change:

```go
func (o *Order) Pay() {
    o.Status = "paid"      // imperative state change
    o.Emit(OrderPaid{...}) // declare what happened
}
```

The buffer is unexported and transient; it is never serialized (persistence is
owned by a per-entity marshaler that maps fields explicitly, so it never touches
`e`).

### 2. `events()` sealed into the `Entity` interface

```go
type Entity interface {
    ID() string
    Type() string
    Version() Version
    SetVersion(Version)
    events() *Events // unexported → only ember-rooted types satisfy Entity
}
```

Consequences:
- `Store` reads/clears the buffer directly (`e.events().All()`,
  `e.events().Clear()`) — no `any(e).(...)` type assertion.
- Because the method is unexported and declared in `ember`, **only types
  embedding `EntityRoot` can satisfy `Entity`**. External packages can no longer
  hand-roll an `Entity`. This guarantees every entity has the buffer (same
  sealing idiom as spark's `NonAtomic`/`Result`).
- All existing entities embed `EntityRoot`, so they satisfy it automatically —
  no migration.

### 3. `Transactor` in ember, reentrant mongo implementation

Ember defines its own transactor interface (spark-shaped, no spark import):

```go
type Transactor interface {
    WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
```

A new `ember/mongo` transactor implements it **reentrantly**: if `ctx` already
carries a mongo session (an outer `WithinTx`, or spark's `Atomic` already opened
one), it joins that transaction and just runs `fn(ctx)`; otherwise it starts a
session and wraps `fn` in `WithTransaction`. Reentrancy is required so a
`Store.Save` nested inside spark's `Atomic` does not attempt an illegal nested
mongo transaction.

Detection uses the mongo v2 driver's session-from-context (verify exact API
during implementation, e.g. `mongo.SessionFromContext(ctx) != nil`).

### 4. `EventStore` — the persist-only event writer

`Publisher` today does two jobs: build+persist the event envelope **and** call
`Notifier.Notify`. For a transactional unit the notify must never fire inside the
tx (with a delivering `Notifier` like `RetryingNotifier` that would publish
uncommitted events → phantom events on rollback). The persist half is also the
exact mirror of `EntityStore`: build the payload, persist it, nothing else.

Introduce `EventStore` — the persist-only counterpart, no notifier:

```go
// EventStore is to events what EntityStore is to entities: it builds the
// envelope and persists it to the EventRepository. It never delivers.
type EventStore struct {
    ider      IDer
    repository EventRepository
    metadata  MetadataGetter
    marshaler EventMarshaler
}

func NewEventStore(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler) *EventStore

func (s *EventStore) Save(ctx context.Context, events ...Event) error // envelope build + repo.Save
```

Delivery stays entirely the relay's job (`mongo.Notifier`'s background poller
drains the `EventRepository` and pushes to the broker). `EventStore` — and the
`Store` UoW built on it — hold no notifier, so they *cannot* deliver mid-tx. The
phantom-event failure mode is structurally impossible, not merely avoided by
discipline.

`Publisher` (build envelope + `Notifier.Notify`, immediate delivery, no persist)
stays as-is for this slice; see Follow-up. In the interim `EventStore` and
`Publisher` share the envelope-building (extract a small private builder) rather
than duplicating it.

### 5. `Store[E]` — the atomic unit (opt-in)

`EntityStore[E]` stays snapshot-only with its existing 2-arg constructor.
Eventless CRUD entities keep using it unchanged. A new composition type persists
an entity and the events it produced in one transaction:

```go
type Store[E Entity] struct {
    entities *EntityStore[E]
    events   *EventStore
    tx       Transactor
}

func NewStore[E Entity](es *EntityStore[E], ev *EventStore, tx Transactor) *Store[E]

func (s *Store[E]) Get(ctx context.Context, id string) (E, error) { return s.entities.Get(ctx, id) }
func (s *Store[E]) List(ctx context.Context, f Filter, sort Sort) ([]E, error) {
    return s.entities.List(ctx, f, sort)
}

func (s *Store[E]) Save(ctx context.Context, e E) error {
    err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
        if err := s.entities.save(ctx, e); err != nil { // guardless: version bump + snapshot
            return err
        }
        evs := e.events().All()
        if len(evs) == 0 {
            return nil
        }
        return s.events.Save(ctx, evs...) // outbox write joins the same tx
    })
    if err != nil {
        return err
    }
    e.events().Clear() // committed — safe to drain; retry keeps events on failure
    return nil
}
```

`Store.Save` reuses the entity snapshot logic (version bump + collapse) inside
the tx via the unexported `save` (see §6), then persists the buffered events to
the `EventStore` in the same transaction. **No notify** — the relay delivers
post-commit by polling. The buffer is cleared **only after a successful commit**,
so a failed commit leaves the events intact for a retry.

### 6. Split snapshot logic; guard the public `EntityStore.Save`

`Store.Save` must persist the snapshot **before** clearing the buffer, so it
cannot call a guarded `EntityStore.Save` (the entity still holds pending events
at that moment). Extract the snapshot logic into an unexported, guardless
method that both entry points share:

```go
// unexported: the actual snapshot persistence (version bump, marshal, repo Save,
// version collapse). No event awareness. Used by Store.Save inside the tx.
func (s *EntityStore[E]) save(ctx context.Context, e E) error { /* existing body */ }

// public: snapshot-only stores must not silently drop events.
func (s *EntityStore[E]) Save(ctx context.Context, e E) error {
    if len(e.events().All()) > 0 {
        return ErrUnpublishedEvents // has pending events — use ember.Store
    }
    return s.save(ctx, e)
}
```

`Store` is in package `ember`, so it calls the unexported `save` directly.
`ErrUnpublishedEvents` turns a silent, hard-to-trace inconsistency into a loud
error the first time an entity with pending events is saved through the wrong
(snapshot-only) store.

## Data flow

```
Order.Pay()            -> mutate state, o.Emit(OrderPaid)
Store.Save(ctx, o)
  tx.WithinTx(ctx):
    EntityStore.save   -> version bump, marshal, entity repo Save (in tx)
    EventStore.Save    -> marshal events, event repo Save (in tx) — NO notify
  commit
  o.events().Clear()
mongo.Notifier relay   -> polls EventRepository, pushes to broker, marks published
(async, post-commit)
```

## Error handling

- `EntityStore.save` inside the tx: version conflict (`ErrVersionConflict`) or
  marshal/repo error → tx fn returns error → `WithinTx` rolls back → buffer NOT
  cleared → caller can retry.
- `EventStore.Save` error → rollback, buffer intact.
- Commit error → `WithinTx` returns error, buffer intact.
- Plain `EntityStore.Save` with pending events → `ErrUnpublishedEvents`, no
  persistence.

## Testing

- `Events`: `Emit` accumulates; `All` clones (mutating the result does not affect
  the buffer); `Clear` empties; `Emit` after `Clear` works (nil-slice append).
- `EntityRoot`: `Emit` then `events().All()` returns recorded events; identity /
  version accessors unchanged.
- `EventStore.Save`: builds one envelope per event (id/entity-id/metadata/
  timestamp) and calls `EventRepository.Save`; propagates marshal/metadata/repo
  errors. Never calls a notifier (it holds none).
- `Store.Save` (real/mock `EntityStore` + mock `Transactor` + mock `EventStore`
  deps):
  - happy path: entity saved, events persisted, buffer cleared, all within one
    `WithinTx` call; no delivery invoked.
  - entity save fails: `EventStore.Save` not called, buffer NOT cleared, error
    propagated.
  - event save fails: error propagated, buffer NOT cleared.
  - no events: `EventStore.Save` not called, buffer cleared, no error.
- `EntityStore.Save` guard: entity with pending events → `ErrUnpublishedEvents`;
  entity with none → existing behavior.
- Reentrant `Transactor` (mongo): integration test — nested `WithinTx` joins the
  outer session rather than opening a second transaction. Unit-level, assert the
  reentrant branch runs `fn` without starting a session when the ctx already
  carries one.

## Files

- `ember/events.go` — new `Events` type.
- `ember/entity.go` — `EntityRoot` fields/methods, `Entity` interface gains
  `events()`, `EntityStore.save`/`Save` split, `ErrUnpublishedEvents` guard,
  `Store[E]` + `NewStore`.
- `ember/event.go` — new `EventStore` + `NewEventStore` (persist-only); extract
  the envelope-builder shared with `Publisher`.
- `ember/transactor.go` — new `Transactor` interface.
- `ember/mongo/transactor.go` — reentrant mongo implementation.
- Tests alongside each.

## Follow-up (separate, breaking PR — out of this slice)

Sequencing (ii): land the UoW ember-only and non-breaking first, then a second PR
does the honest publisher/relay split:

- `Publisher` sheds persistence — becomes build-envelope + `Notifier.Notify`
  only (immediate-delivery path). `NewPublisher` drops the `EventRepository` arg.
- `mongo.Notifier` → `mongo.Relay`: sheds the no-op `Notify` and the `Notifier`
  interface conformance; keeps `Run`/`tick`/`publishBatch`. It is a relay, not a
  notifier.
- `Notifier` interface then only has delivering impls (`RetryingNotifier`,
  `noop`).
- Breaking change to ember's public API → updates every consuming service's
  `main.go` wiring + an ember version bump (same publish → `go get` flow as
  spark). This is why it is deferred out of the UoW slice.
