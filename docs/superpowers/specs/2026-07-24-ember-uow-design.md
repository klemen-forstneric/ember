# Ember Unit of Work: atomic entity save + event publish

**Date:** 2026-07-24
**Scope:** `ember` library only. This changes ember's public API (the entity read/write surface) — a **breaking** change; consuming services rewire at adoption, a later separate step.

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

- No event sourcing: state is still mutated imperatively and persisted as a
  snapshot; reads return the snapshot, never replay events.
- No changes to the outbox relay (`ember/mongo` notifier) — it already delivers
  post-commit by polling.
- No changes to `Publisher`'s constructor or `mongo.Notifier` in this slice; the
  publisher/relay rename is a deferred follow-up (see end).
- No adoption in the consuming services. That is a later, separate step.

## Read/write split (the shape)

Reads need the concrete type `E` (to unmarshal); writes do not — an entity knows
its own `Type()` and carries its own `events()`. So the surface splits:

- `Binding[E]` — the single declaration of a type's persistence (repository +
  marshaler), produced by `Bind[E]`. Feeds both sides; declared once per type.
- `EntityLoader[E]` — typed reads (`Get`/`List`), built from a `Binding[E]`.
- `EntitySaver` — non-generic; `Save(ctx, ...Entity)` persists any registered
  entity(ies) + their events in one transaction. Built from `Binding` values.
- `EntityStore[E]` — convenience pairing a loader + the shared saver for the
  common single-type case (`Get`/`List`/`Save`).

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
- `EntitySaver` reads/clears the buffer directly (`e.events().All()`,
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
session and wraps `fn` in `WithTransaction`. Reentrancy is required so an
`EntitySaver.Save` nested inside spark's `Atomic` does not attempt an illegal
nested mongo transaction.

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
`EntitySaver` built on it — hold no notifier, so they *cannot* deliver mid-tx.
The phantom-event failure mode is structurally impossible, not merely avoided by
discipline.

`Publisher` (build envelope + `Notifier.Notify`, immediate delivery, no persist)
stays as-is for this slice; see Follow-up. In the interim `EventStore` and
`Publisher` share the envelope-building (extract a small private builder) rather
than duplicating it.

### 5. `Binding[E]` — one declaration of a type's persistence

`Binding[E]` is a plain value holding a type's repository + marshaler. It is the
single source both the read side and the write side are built from — declared
once per entity type and passed to whichever constructor needs it.

```go
type Binding[E Entity] struct {
    repo      EntityRepository
    marshaler EntityMarshaler[E]
}

func Bind[E Entity](r EntityRepository, m EntityMarshaler[E]) Binding[E] {
    return Binding[E]{repo: r, marshaler: m}
}

// binding is the type-erased view the saver consumes; sealed so only ember's
// Binding[E] can satisfy the saver's variadic (same idiom as Entity.events()).
type binding struct {
    typ     string
    repo    EntityRepository
    marshal func(ctx context.Context, e Entity) (*MarshaledEntity, error)
}

func (b Binding[E]) binding() binding {
    var zero E
    return binding{
        typ:  zero.Type(), // Type() is a constant method; safe on the nil zero value
        repo: b.repo,
        marshal: func(ctx context.Context, e Entity) (*MarshaledEntity, error) {
            return b.marshaler.Marshal(ctx, e.(E))
        },
    }
}
```

`Bind[E]` is a value constructor, not a registration side effect. No public
`.Loader()` method on the binding — you pass the value to a constructor.

### 6. `EntityLoader[E]` — typed reads

```go
type EntityLoader[E Entity] struct {
    repository EntityRepository
    marshaler  EntityMarshaler[E]
}

func NewEntityLoader[E Entity](b Binding[E]) *EntityLoader[E] {
    return &EntityLoader[E]{repository: b.repo, marshaler: b.marshaler}
}

func (l *EntityLoader[E]) Get(ctx context.Context, id string) (E, error)          // unchanged read logic
func (l *EntityLoader[E]) List(ctx context.Context, f Filter, s Sort) ([]E, error) // (moved from EntityStore)
```

`Get`/`List` are the existing `EntityStore` read bodies, verbatim.

### 7. `EntitySaver` — the atomic write unit

Non-generic. Built from `Binding` values (each contributes one type→binding entry
via the sealed `binding()`). `Save` persists any registered entity(ies) plus the
events they produced, atomically, and never touches a notifier.

```go
type EntitySaver struct {
    bindings map[string]binding
    events   *EventStore
    tx       Transactor
}

// binder is the sealed interface Binding[E] satisfies (unexported → ember-only).
type binder interface{ binding() binding }

func NewEntitySaver(ev *EventStore, tx Transactor, bindings ...binder) *EntitySaver {
    m := make(map[string]binding, len(bindings))
    for _, b := range bindings {
        bd := b.binding()
        m[bd.typ] = bd
    }
    return &EntitySaver{bindings: m, events: ev, tx: tx}
}

func (s *EntitySaver) Save(ctx context.Context, entities ...Entity) error {
    var events []Event
    for _, e := range entities {
        events = append(events, e.events().All()...)
    }

    // pending pairs each entity with the version to adopt once the write is durable.
    type pending struct {
        entity  Entity
        version Version
    }
    var pend []pending

    work := func(ctx context.Context) error {
        pend = pend[:0]
        for _, e := range entities {
            v, err := s.persist(ctx, e)
            if err != nil {
                return err
            }
            pend = append(pend, pending{e, v})
        }
        if len(events) > 0 {
            return s.events.Save(ctx, events...)
        }
        return nil
    }

    var err error
    if len(entities) == 1 && len(events) == 0 {
        err = work(ctx) // single write, no events: no transaction needed
    } else {
        err = s.tx.WithinTx(ctx, work)
    }
    if err != nil {
        return err // persist never mutated any entity — nothing to restore
    }

    for _, p := range pend {
        p.entity.SetVersion(p.version)
        p.entity.events().Clear()
    }
    return nil
}

// persist marshals the snapshot at the next version and writes it, WITHOUT
// permanently mutating e; it returns the (collapsed) version to adopt on commit.
func (s *EntitySaver) persist(ctx context.Context, e Entity) (Version, error) {
    b, ok := s.bindings[e.Type()]
    if !ok {
        return Version{}, fmt.Errorf("%w: %s", ErrUnregisteredEntity, e.Type())
    }
    prev := e.Version()
    next := prev.Inc()
    e.SetVersion(next)
    m, err := b.marshal(ctx, e)
    e.SetVersion(prev) // leave e untouched regardless of outcome
    if err != nil {
        return prev, err
    }
    if err := b.repo.Save(ctx, m); err != nil {
        return prev, err
    }
    return NewVersion(next.Value()), nil
}
```

Key properties:
- **No in-tx mutation of `e`.** `persist` sets the version only to marshal the
  snapshot, then restores it; the version-advance and buffer-`Clear` are applied
  **once, after commit**, together. A rollback leaves entities untouched — no
  restore needed, no band-aid.
- **No notify.** The relay delivers post-commit by polling.
- **tx only when needed.** One entity with no events is a single write, no
  transaction. Multiple entities (cross-type atomicity) or any events → one
  transaction (reentrant: joins an outer/spark tx).
- **Unregistered type → `ErrUnregisteredEntity`.** The honest failure for saving
  a type whose `Binding` was never given to the saver.

### 8. `EntityStore[E]` — single-type convenience

Self-contained: pairs a typed loader with an internal single-type saver it builds
itself, restoring the familiar `Get`/`List`/`Save` surface for the common case.
No `Bind` ceremony at the call site — pass repo + marshaler directly, plus the
shared `EventStore`/`Transactor` infra.

```go
type EntityStore[E Entity] struct {
    loader *EntityLoader[E]
    saver  *EntitySaver
}

func NewEntityStore[E Entity](r EntityRepository, m EntityMarshaler[E], ev *EventStore, tx Transactor) *EntityStore[E] {
    b := Bind[E](r, m)
    return &EntityStore[E]{loader: NewEntityLoader(b), saver: NewEntitySaver(ev, tx, b)}
}

func (s *EntityStore[E]) Get(ctx context.Context, id string) (E, error)          { return s.loader.Get(ctx, id) }
func (s *EntityStore[E]) List(ctx context.Context, f Filter, srt Sort) ([]E, error) { return s.loader.List(ctx, f, srt) }
func (s *EntityStore[E]) Save(ctx context.Context, e E) error                    { return s.saver.Save(ctx, e) }
```

The store's internal saver knows only `E`. The public `EntitySaver` (§7) is the
separate tool for cross-type atomic saves. A type used both ways (its own store +
a shared saver) is wired in both places — rare, and cross-type is the exception.

## Wiring

```go
// common single-type case — self-contained store, no Bind ceremony
orders  := ember.NewEntityStore[*Order](orderRepo, orderMarshaler, eventStore, tx)
wallets := ember.NewEntityStore[*Wallet](walletRepo, walletMarshaler, eventStore, tx)

orders.Get(ctx, id)
orders.Save(ctx, order)          // persists order + its events atomically

// cross-type atomic — explicit shared saver built from bindings
saver := ember.NewEntitySaver(eventStore, tx,
    ember.Bind[*Order](orderRepo, orderMarshaler),
    ember.Bind[*Wallet](walletRepo, walletMarshaler),
)
saver.Save(ctx, order, wallet)

// reads-only alternative
ol := ember.NewEntityLoader(ember.Bind[*Order](orderRepo, orderMarshaler))
```

## Data flow

```
Order.Pay()            -> mutate state, o.Emit(OrderPaid)
saver.Save(ctx, o)     (via orders.Save)
  events := o.events().All()
  tx.WithinTx(ctx):    (skipped: 1 entity + 0 events -> plain write)
    persist(o)         -> marshal snapshot at next version (e restored), entity repo Save
    EventStore.Save    -> marshal events, event repo Save (in tx) — NO notify
  commit
  o.SetVersion(next); o.events().Clear()   (both applied only post-commit)
mongo.Notifier relay   -> polls EventRepository, pushes to broker, marks published
(async, post-commit)
```

## Error handling

- `persist` (marshal/repo error) or `EventStore.Save` error → `work` returns
  error → `WithinTx` rolls back → **no** version-advance, **no** buffer-clear
  applied (both are post-commit only) → entities untouched → caller can retry the
  same instance.
- Commit error → `WithinTx` returns error, same as above: entities untouched.
- Saving a type whose `Binding` was not given to the saver → `ErrUnregisteredEntity`.
- Optimistic-concurrency conflict from the repo surfaces as `ErrVersionConflict`,
  unchanged.

## Testing

- `Events`: `Emit` accumulates; `All` clones (mutating the result does not affect
  the buffer); `Clear` empties; `Emit` after `Clear` works (nil-slice append).
- `EntityRoot`: `Emit` then `events().All()` returns recorded events; identity /
  version accessors unchanged.
- `EventStore.Save`: builds one envelope per event (id/entity-id/metadata/
  timestamp) and calls `EventRepository.Save`; propagates marshal/metadata/repo
  errors. Never calls a notifier (it holds none).
- `Binding`/`Bind`: `binding()` reports the entity's `Type()`, and its `marshal`
  closure dispatches to the typed marshaler.
- `EntityLoader.Get`/`List`: the existing read tests, moved.
- `EntitySaver.Save` (real `EntityLoader`/`EventStore` over mock repos + a
  `mockTransactor` that runs the callback):
  - single entity + events: entity persisted, events persisted, version advanced,
    buffer cleared, all inside one `WithinTx`; no delivery invoked.
  - single entity, no events: persisted with **no** `WithinTx` call.
  - two entities of different types: both persisted + combined events, one
    `WithinTx`, both buffers cleared.
  - persist failure and event-save failure: error propagated, **entities
    untouched** (version unchanged, buffer intact) → a same-instance retry
    succeeds.
  - commit (WithinTx) error with the body succeeding: entities untouched.
  - unregistered entity type → `ErrUnregisteredEntity`, nothing persisted.
- `EntityStore[E]`: `Get`/`List` delegate to the loader; `Save` delegates to the
  saver (one entity path).
- Reentrant `Transactor` (mongo): the reentrant branch runs `fn` on the existing
  session without starting a second one (asserts session identity); SKIPs without
  local mongo.

## Files

- `ember/events.go` — `Events` type.
- `ember/entity.go` — `EntityRoot` fields/methods, `Entity` interface gains
  `events()`. (The old `EntityStore` snapshot type is removed from here.)
- `ember/binding.go` — `Binding[E]`, `Bind`, sealed `binding`/`binder`.
- `ember/loader.go` — `EntityLoader[E]` + `NewEntityLoader` (the moved `Get`/`List`).
- `ember/saver.go` — `EntitySaver`, `NewEntitySaver`, `persist`, `ErrUnregisteredEntity`.
- `ember/store.go` — `EntityStore[E]` convenience (loader + saver).
- `ember/event.go` — `EventStore` + `NewEventStore`; shared envelope-builder.
- `ember/transactor.go` — `Transactor` interface.
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
