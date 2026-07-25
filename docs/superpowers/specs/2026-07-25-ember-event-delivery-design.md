# ember event delivery redesign

Date: 2026-07-25
Status: approved, not implemented

## Problem

Event delivery is modeled around `Notifier`, which conflates two orthogonal concerns:
*when* delivery happens (inline vs background) and *how* it happens (transport).

Current shape:

- `Publisher.Publish` does three jobs — build envelopes, `EventRepository.Save`, `Notifier.Notify`.
- `Notifier` has three implementations. `ext.RetryingNotifier` delivers inline with backoff.
  `noop.Notifier` drops. `mongo.Notifier` is not a notifier at all — it is a background
  poller draining the outbox, and its `Notify` is an empty method implemented solely to
  satisfy the interface.
- `Notify` returns no error, so a failed inline delivery is invisible to the caller.
- Choosing whether to use the outbox means picking a *pair* of collaborators:
  `mongo.EventRepository` + `mongo.Notifier`, or `noop.EventRepository` +
  `ext.RetryingNotifier`. Mismatched pairs compile and fail silently —
  `mongo.EventRepository` + `noop.Notifier` leaves events in the outbox forever.
- The unit-of-work path (`EntitySaver` → `EventStore`) has no delivery seam at all, so
  non-outbox delivery cannot be expressed there.

ember is library code. Every supported topology must be expressible, and no configuration
may silently break.

## Decisions

**Guarantee vocabulary, not mechanism.** The choice a user makes at wiring time is a
delivery guarantee, so the constructors are named for guarantees:

```go
func AtLeastOnce(i IDer, r EventRepository, mg MetadataGetter, m EventMarshaler) *Publisher
func BestEffort(i IDer, s Sink, mg MetadataGetter, m EventMarshaler) *Publisher
```

Naming these `Outbox`/`Immediate` (mechanism) or `Deferred`/`Immediate` (timing) invites the
wrong inference — everyone wants immediate delivery with strong guarantees, and mechanism
names hide the tradeoff being made. `Deferred` would also go stale the moment a relay nudge
lands.

**One knob, closed set.** The guarantee is chosen by constructor and stored behind an
unexported interface. Users cannot assemble a broken combination, and cannot add a fourth
mode that voids ordering.

**No `Both` mode.** Persist *and* push inline, with the relay as a failure backstop, was
considered and rejected. It creates two publishers for one stream: an event whose inline
push fails falls to the relay and is delivered *after* a later event for the same entity
that pushed successfully. That voids per-entity ordering — the invariant the pending
ordering redesign exists to guarantee. Making it safe requires a distributed per-entity
catch-up gate consulted on every publish. The only motivation was latency, which the
deferred relay nudge captures without the hazard.

**No `Transactor` hooks, no ctx-carried state.** An `AfterCommit(ctx, fn)` registry stored
in `context.Context` was designed and rejected: it smuggles a mutable collector through ctx,
and it forces every backend transactor to reimplement hook plumbing. Deferral is an explicit
return value instead.

**Symmetric transport naming.** `ext.Transport` (publish side) and `ember.Transport`
(subscribe side) are unrelated interfaces sharing a name. They become `Sink` and `Source`.

## Design

### Core seam

```go
// delivery is work that must run only after the transaction commits.
type delivery func(ctx context.Context) error

// guarantee is the delivery guarantee a Publisher was built with. Unexported, so the
// set of guarantees is closed.
type guarantee interface {
    // stage performs the guarantee's durable work inside the caller's transaction and
    // returns any delivery deferred until after commit.
    stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error)
}

type atLeastOnce struct{ repo EventRepository }

func (g atLeastOnce) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
    return nil, g.repo.Save(ctx, envelopes) // Relay delivers; nothing waits on commit
}

type bestEffort struct{ sink Sink }

func (g bestEffort) stage(ctx context.Context, envelopes []EventEnvelope) (delivery, error) {
    return func(ctx context.Context) error { // nothing to persist; deliver post-commit
        return g.sink.Publish(ctx, envelopes)
    }, nil
}

type Publisher struct {
    builder   envelopeBuilder
    guarantee guarantee
}
```

`Publisher.stage` builds envelopes and delegates to the guarantee. It is unexported, so only
`EntitySaver` — same package — can drive delivery separately from staging:

```go
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
```

`Publish` is the entity-less path. No transaction is in scope, so a deferred delivery runs
immediately:

```go
func (p *Publisher) Publish(ctx context.Context, events ...Event) error {
    d, err := p.stage(ctx, events...)
    if err != nil || d == nil {
        return err
    }
    return d(ctx)
}
```

Each guarantee's `stage` does real work — `atLeastOnce` returns "nothing waits on commit",
`bestEffort` returns "everything waits on commit". Neither has an empty method.

There are no variadic options in this revision. Retry belongs to `ext.RetryingSink`, and the
relay nudge is deferred, so nothing is left to configure.

### EntitySaver integration

`EntitySaver` holds a `*Publisher` in place of `*EventStore`. It calls the same unexported
seam inside its transaction and runs the returned delivery in the post-commit block it
already has:

```go
var d delivery

fn := func(ctx context.Context) error {
    d = nil
    saved = nil
    for _, e := range es {
        v, err := s.save(ctx, e)
        if err != nil {
            return err
        }
        saved = append(saved, entities{e: e, v: v})
    }
    var err error
    d, err = s.publisher.stage(ctx, events...)
    return err
}

if err := s.tx.WithinTx(ctx, fn); err != nil {
    return err
}

for _, p := range saved {
    p.e.SetVersion(p.v)
    p.e.events().Clear()
}

if d != nil {
    if err := d(context.WithoutCancel(ctx)); err != nil {
        return fmt.Errorf("%w: %w", ErrDeliveryFailed, err)
    }
}
return nil
```

One call site serves both guarantees: `atLeastOnce`'s `repo.Save` joins the transaction,
`bestEffort`'s delivery is returned unrun and fires after commit.

Version bump and buffer clear happen **before** delivery and unconditionally. The
transaction committed, so the entity must match durable state; doing it the other way leaves
a committed entity carrying a stale version and un-cleared events, and a retry re-emits.

`context.WithoutCancel` prevents a client disconnect after commit from skipping delivery.

### Error semantics

`ErrDeliveryFailed` distinguishes "state committed, delivery failed" from "nothing
happened". It is reachable only under `BestEffort` — that is the dual-write the guarantee
name advertises. Under `AtLeastOnce` there is no post-commit delivery to fail; the relay
retries indefinitely.

`Publish` now returns delivery errors. `Notifier.Notify` returned nothing, so
`RetryingNotifier` exhausting its backoff meant a logged-and-dropped event.

### Relay

`mongo.Notifier` moves to core as `Relay`, backend-agnostic, poller unchanged:

```go
type RelayConfig struct {
    IdleInterval time.Duration // idle poll cadence (jittered per replica)
    BatchSize    int           // events fetched per round
    LockKey      string        // redis efficiency-lock key
    LockTTL      time.Duration // lock lease; must exceed a bounded round
    Retention    time.Duration // published_at + Retention → expires_at (TTL)
}

func NewRelay(r EventRepository, s Sink, l Locker, log LoggerCtx, cfg RelayConfig) *Relay
func (r *Relay) Run(ctx context.Context)
func (r *Relay) Close() error
```

Retains the redis efficiency lock, jittered interval, batch drain, and the per-entity
failure isolation (`failed[e.EntityID]` skips later events for an entity whose earlier event
failed) that preserves per-entity order. `Notify` is deleted along with the interface it
existed to satisfy.

`postgres.EventRepository` already satisfies `EventRepository` in full, so a postgres relay
becomes wiring-only — still gated on the ordering fix, which must land before any relay
drains that outbox.

### Locker moves to core

`Relay` needs `Locker`, but `ember/middleware` imports `ember`, so core cannot reference
`middleware.Locker`. Declaring a second interface in core does not work either:
`redis.Locker.TryLock` returns the concrete `middleware.Lock`, and Go requires identical
return types.

`Lock` and `Locker` move to core; `middleware` keeps the old names as aliases:

```go
// ember/lock.go
type Lock interface {
    Release(ctx context.Context) error
}

type Locker interface {
    TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}

// ember/middleware/idempotent.go
type Lock = ember.Lock
type Locker = ember.Locker
```

Aliases are identical types, so `redis` and `middleware.Idempotent` need no changes.

### Sink and Source

```go
type Sink interface {
    Publish(ctx context.Context, envelopes []EventEnvelope) error
}

type Source interface {
    Subscribe(ctx context.Context, name string) (<-chan AckableEventEnvelope, error)
    Stop()
}
```

Both in core. `pulsar.Publisher` and `kafka.Publisher` implement `Sink`;
`pulsar.Subscriber` and `kafka.Subscriber` implement `Source`. `Subscriber`'s field and
parameter names follow.

### One outbox interface

`EventRepository` grows the drain side rather than gaining a sibling:

```go
type EventRepository interface {
    Save(ctx context.Context, envelopes []EventEnvelope) error
    ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error)
    MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}
```

The narrow-interface idiom would declare each half at its consumption site — `Save` for
`AtLeastOnce`, the drain pair for `Relay`. Rejected: it makes a `Save`-only repository wired
to `AtLeastOnce` a legal configuration, and that configuration accumulates events nobody ever
delivers. Same silent-failure class as the old `mongo.EventRepository` + `noop.Notifier`
pairing. `AtLeastOnce` requiring a *drainable* outbox is the guarantee stated in the type.

Nothing implements `Save` alone once `noop.EventRepository` is deleted, and both mongo and
postgres repositories already implement all three. mongo's package-local `eventRepository`
is deleted rather than promoted — it was a duplicate of what core should have declared.

## Deletions and renames

| removed | replacement |
| --- | --- |
| `ember.Notifier` | nothing — no delivery strategy left to abstract |
| `ext.RetryingNotifier` | `ext.RetryingSink`, a `Sink` decorator |
| `mongo.Notifier`, `NotifierConfig`, `NewNotifier` | `ember.Relay`, `RelayConfig`, `NewRelay` |
| `noop.Notifier`, `noop.EventRepository`, the `noop` package | `BestEffort` is the no-outbox configuration |
| `ember.EventStore`, `NewEventStore` | `atLeastOnce.stage` |
| `ember.NewPublisher` | `AtLeastOnce` / `BestEffort` |
| `ext.Transport`, `ember.Transport` | `Sink` / `Source` |

`EntityStore[E]`'s constructor takes `*Publisher` where it took `*EventStore`.

## Wiring

```go
// before
notifier := embermongo.NewNotifier(outboxRepository, pulsarPublisher,
    emberredis.NewLocker(redisClient), logger,
    embermongo.NotifierConfig{LockKey: "order-service:outbox:relay"})
publisher := ember.NewPublisher(&skuuid.IDer{}, outboxRepository,
    &ext.MetadataGetter{}, eventMarshaler, notifier)
go notifier.Run(ctx)

// after
relay := ember.NewRelay(outboxRepository, pulsarPublisher,
    emberredis.NewLocker(redisClient), logger,
    ember.RelayConfig{LockKey: "order-service:outbox:relay"})
publisher := ember.AtLeastOnce(&skuuid.IDer{}, outboxRepository,
    &ext.MetadataGetter{}, eventMarshaler)
go relay.Run(ctx)
```

The relay is no longer injected into the publisher. They are independent, connected only by
the shared repository. Switching guarantees means calling
`ember.BestEffort(&skuuid.IDer{}, pulsarPublisher, &ext.MetadataGetter{}, eventMarshaler)`
and deleting the relay.

## Transaction ownership

This design assumes ember owns the outermost transaction. Today every service wires
`sparkmw.Atomic(sparkmongo.NewTransactor(client))`, which opens a handler-wide transaction
ember cannot see — under that topology `EntitySaver`'s post-commit block is nominal, running
after a *joined* transaction returns rather than after the real commit. `spark.NonAtomic`
appears nowhere in the monorepo, so every command runs in a transaction, including
`order-service`'s `Process`, which makes a blocking HTTP call to payment-service at
`internal/order/service.go:143` while holding one.

Nothing is deployed yet, so this spec targets the end state: transactions scoped to
`EntitySaver.Save`, not to the handler. Retiring `sparkmw.Atomic` and migrating services to
`Emit` + `EntitySaver` is a separate spec. The design is correct under either topology for
`AtLeastOnce`; only `BestEffort` inside a caller-owned transaction publishes pre-commit,
which is inherent to immediate delivery and documented rather than prevented.

## Testing

- `mongo/notifier_test.go` becomes `relay_test.go` in core, against a mocked
  `EventRepository`, `Sink`, and `Locker` — one mock serves the publisher and relay suites.
  Existing coverage carries over: batch drain,
  per-entity failure isolation, lock contention, interval jitter, `Close`.
- `publisher_test.go` covers both guarantees: `AtLeastOnce` persists and returns no
  delivery; `BestEffort` persists nothing and returns a delivery that pushes.
- `saver_test.go` extends with a `BestEffort` publisher, asserting delivery runs after
  commit, that version bump and buffer clear happen even when delivery fails, and that the
  error is wrapped as `ErrDeliveryFailed`.
- `ext` gains `retrying_sink_test.go` replacing the notifier test.
- Existing suites use `testify/suite` with `mock.Mock` doubles; new tests follow suit in the
  canonical file for each unit.

## Out of scope

- **Event ordering redesign.** `seq = Timestamp.UnixNano()` → per-entity
  `(version, intra-save index)`. Untouched here; the relay still drains `ORDER BY seq`. Both
  changes are breaking and reshape the same subsystem, so they can ship in one release as
  separate specs.
- **`sparkmw.Atomic` retirement and per-service UoW adoption.** Its own spec. Dropping
  `Atomic` before a service moves to `Emit` + `EntitySaver` would regress that service from
  atomic to a genuine dual-write, since `orders.Save` and `publisher.Publish` are two calls.
- **Relay nudge.** Cut from this revision. With the `delivery` continuation it is a
  later addition with no API break: `atLeastOnce.stage` returns a delivery that wakes the
  relay. Costs ~300ms mean added latency in the meantime, which is the status quo.
- **Event log mode.** flux persists events *and* pushes immediately, because its event
  records are a log, not a queue — order-safe there, since it has no relay racing the
  notifier. ember's `EventRepository` is an outbox with TTL retention, so a log mode means
  retention policy, replay, and no TTL. A different feature, not a delivery guarantee.

## RetryingSink bounds tries, not elapsed time

`MaxElapsedTime` defaulting to `-1` meant backoff/v4 stopped before the first retry — a type
called "Retrying" that did not retry unless configured. The replacement bounds attempts:

```go
type RetryingSinkConfig struct {
    InitialInterval time.Duration // default 100ms
    MaxInterval     time.Duration // default 1s
    MaxTries        int           // total attempts including the first; default 3, 1 disables retrying
}
```

Tries rather than retries, because "3 retries" invites an off-by-one at every call site.
`int`, not backoff's `uint` — the dependency's choice of unsigned is an implementation detail
converted at the single call site, not something the config should make callers deal with.
Defaulting is then uniform across all three fields (`<= 0` applies the default), and since the
default guarantees `MaxTries >= 1`, the conversion to `uint` cannot wrap.

**backoff upgrades v4.3.0 → v7.0.0**, where this is the native vocabulary:
`WithMaxTries(n)` bounds *total attempts*, so `MaxTries` maps straight through with no
arithmetic, and `WithMaxTries(1)` runs once without retrying. v7 also takes `ctx` as `Retry`'s
first argument, which fixes a real bug: the current call is
`backoff.RetryNotify(publish, b, notify)` with no context at all, so a cancelled request keeps
retrying. v7 checks `context.Cause(ctx)` between attempts and interrupts the backoff wait.

`WithMaxElapsedTime(0)` is passed explicitly, because v7 defaults it to
`DefaultMaxElapsedTime` (15 minutes) and both limits are otherwise active at once. The try
count is the only bound we want.

The default of 3 tries is short on purpose. `Publish` blocks its caller — a command handler on
path A, or `EntitySaver` immediately after commit on path B — so with the default intervals
the worst case is roughly 100ms + 150ms of waiting. A multi-second ceiling would hang a
request through a broker outage.

v7 wraps every failure in `*RetryError`, which does not break error matching: `Unwrap()`
returns `[]error{Cause, LastErr}`, so `errors.Is(err, sinkErr)` still finds the sink's error
and the `ErrDeliveryFailed` chain survives. It also adds `errors.Is(err, backoff.ErrExhausted)`
for distinguishing "gave up" from "context cancelled", which the tests assert.

`RetryingSink` is for `BestEffort`. Wrapping the relay's `Sink` is a mistake worth documenting:
the retry runs inline in `publishBatch` while the redis lock is held, so a retry window
approaching `LockTTL` lets the lock expire mid-round and a second replica start draining —
duplicates and reordering. The relay's own retry is leaving the event unpublished until the
next tick, which never blocks the drain. Sub-second bounds keep this unreachable in practice.

`ext/retrying_notifier.go` is the only file in ember importing backoff, and v7 requires
go 1.23 against ember's 1.26.3, so the upgrade is contained to this one file plus `go.mod`.
