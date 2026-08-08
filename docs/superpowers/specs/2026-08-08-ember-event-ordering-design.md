# ember event ordering redesign

Date: 2026-08-08
Status: approved, not implemented

## Problem

The outbox orders events for delivery by a wall-clock reading. `envelopeBuilder.build`
stamps `Timestamp: time.Now().UTC()` per event (`event.go:82`), both event repositories
persist `seq = e.Timestamp.UnixNano()` (`mongo/event_repository.go:58`,
`postgres/event_repository.go:45`), and the relay drains `ORDER BY seq ASC`.

That does not guarantee the invariant every ember consumer relies on: **events added later
are published later**. Three ways it breaks, all real:

1. **Intra-save ties.** Each event gets its own `time.Now()`. The wall clock is
   non-decreasing, not strictly increasing, and `UnixNano()` reads the wall clock rather
   than the monotonic reading — so two events emitted by a single `Save`, on a single
   entity, can share a nanosecond and be delivered in arbitrary order. Rare on Linux where
   call overhead usually exceeds a clock tick; common on Windows, VMs, and coarse
   clocksources.
2. **Backward clock step.** An NTP correction gives a later event a smaller `seq`.
3. **Multi-writer skew.** A replica with a lagging clock saves the same entity later and
   writes a smaller `seq`. Optimistic concurrency serializes *concurrent* writes, but
   different replicas legitimately save the same entity over time.

ULID does not fix this. A monotonic ULID removes same-millisecond ties within one process,
but it is still wall-clock-anchored, so modes 2 and 3 survive.

The required guarantee is **per-entity ordering**. Entities are independent; no
cross-entity or total order is needed, and the relay already delivers per entity
(`polling_relay.go:97` groups by `EntityID`).

## Decisions

**The ordering key is `(version, index)`, not a timestamp.** ember already maintains a
per-entity, persisted, monotonic counter — `Version`, bumped once per `Save` by
`EntitySaver` (`saver.go:110`). A save can emit several events, so the key pairs that
version with the event's position within the save. Clock-independent, correct across
replicas and clock jumps. A writer cannot produce v2 without having read committed v1, so
commit order equals version order per entity, with no concurrent-commit gap.

**Bumping the version once per event was considered and rejected.** It collapses the key to
a single `bigint` — one column, `ORDER BY version`, simpler everywhere — and it is
mechanically sound: `Inc()` becomes `Add(n)`, OCC still filters on `{_id, version: prev}`
and writes `prev+N`. Three reasons against it:

1. A save emitting no events must still bump by 1, or two concurrent no-event saves both
   pass OCC and lose an update. The delta is `max(1, len(events))`, so the version is not an
   event count — the intuition breaks on the first special case, and those saves exist
   (subscription-service's guarded no-ops).
2. The entity's persisted state starts depending on outbox behavior. Under `BestEffort`
   there is no outbox, yet the version still inflates by event count; if an event is ever
   filtered before staging, the delta and the emitted count diverge.
3. `index` lives only in the outbox, which this migration drains to empty anyway — free.
   Version-per-event changes the meaning of a field in every entity record in production
   across all services, with no migration available: old versions are save counts, new ones
   are event counts, and the mixture is permanent and invisible. Harmless, since only
   monotonicity matters, but it is a semantic change to live data rather than a schema
   change to a table already being emptied.

Underneath all three: event sourcing makes the event stream the source of truth, so a
per-event sequence *is* the aggregate's version — that is what flux's `AggregateSequence`
is. ember is not event-sourced. The entity is the source of truth and events are
notifications about it, so the version belongs to the entity's write history. `index` is an
`int` on an outbox row that TTL deletes a week later — a cheaper home for the intra-save
distinction than the entity's durable identity.

**Version 0 marks the unordered lane.** `NewEntityRoot` starts entities at version 0 and
`saver.go:110` increments before the first save, so no ordered event is ever stamped
version 0. That makes it a free sentinel: `Publisher.Publish` — the entity-less path
(`publisher.go:37`) — has no entity and therefore no version, and stamps 0. Those events
carry no ordering promise and are documented as such. No flag column, no nullable field.

**The delivery group is `entity_id` alone.** Adding `entity_type` to the outbox was
considered and rejected. If two entity types share an id, sorting their merged events by
`(version, index)` is a stable merge of two independent sequences — each entity's
subsequence stays intact, and a `LIMIT` cut still leaves each one a valid prefix.
`pulsar/publisher.go:83` keys on `EntityID`, so both land on the same partition and the
broker preserves that interleaving. Nothing observes the mixing. The group is a delivery
unit, not an entity identity, and it is fine for it to hold more than one entity.

The only thing this would break is a consumer treating `(entity_id, version)` as an
idempotency key and seeing two `v1`s. Nothing does that today, and the version does not
leave ember (below).

**An entity emits only its own events, enforced.** With that rule, the version stamp and
the delivery group are the same thing by construction. `EntitySaver` hard-errors on an
event whose `EntityID()` differs from the emitting entity's `ID()`: a foreign event means
the version stamp is a lie, and failing the `Save` beats silently mis-sequencing.

**The version stays inside ember.** `EventEnvelope` carries it because the outbox must
store it. `ReceivedEvent` does not gain it and no transport forwards it. Exposing it would
let consumers reject stale redeliveries — the standard companion to per-entity ordering —
but no consumer needs that today, and it is an API surface that cannot be withdrawn.

**Fetch order is decoupled from publish order.** `ORDER BY version, index` as a *global*
sort is correct — within an entity v1 always precedes v2, so any prefix of that sort is a
valid prefix per entity — but it starves. An entity at version 8000 sorts behind every
version-1 event in the outbox, and under a backlog, freshly created entities keep arriving
at v1 and keep jumping the queue. The high-version entity is not merely delayed; it can
starve indefinitely. That bites exactly the long-lived, high-churn entities that matter
most.

So the drain is two-phase: sample entities, then drain each in version order. Selection is
**random**, which keeps the clock out of the drain path entirely. Oldest-first by
`created_at` is equally correct here — two-phase cannot cut between v1 and v2 of one
entity, so clock skew in *selection* only mis-prioritizes, never mis-orders — and remains
a swappable policy. Random simply also drops the index and the last clock reference.

Note that "unordered" is not "random": `SELECT DISTINCT entity_id ... LIMIT K` with no
`ORDER BY` returns stable index-scan order, the same K entities every round, which
reinvents starvation. The randomization must be explicit.

## Design

### The key

```go
// EventEnvelope
type EventEnvelope struct {
	ID        string
	EntityID  string
	Version   uint64 // 0 = unordered (Publisher.Publish)
	Index     int
	Event     *MarshaledEvent
	Metadata  Metadata
	Timestamp time.Time
}
```

`Timestamp` stays, demoted to observability — `polling_relay.go:112` logs `elapsed_ms` off
it. Nothing sorts by it again.

### Stamping

`saver.go:40` flattens events from every entity into one bag, losing the entity
association. It flattens into a stamped slice instead. The version is available before the
save loop; `s.save` computes the same value at `saver.go:110`.

```go
type staged struct {
	event   Event
	version uint64
	index   int
}

var events []staged
for _, e := range es {
	v := e.Version().Inc().Value()
	for i, ev := range e.events().All() {
		if ev.EntityID() != e.ID() {
			return fmt.Errorf("%w: %s emitted an event for %s", ErrForeignEvent, e.ID(), ev.EntityID())
		}
		events = append(events, staged{event: ev, version: v, index: i})
	}
}
```

`Publisher.stage` and `envelopeBuilder.build` take `[]staged` instead of `...Event`.
`Publisher.Publish` wraps its events with `version: 0, index: i`. Two separate `Publish`
calls both produce `(0, 0)`; they promise nothing, so that is fine.

`staged` is unexported, so the public API change is the two envelope fields and
`ErrForeignEvent`.

`BestEffort` is unaffected: it pushes envelopes to the sink in `Save` order, which is
already correct. Its envelopes carry the version unused.

### The drain

`PollingRelayRepository.ListUnpublished(ctx, limit)` cannot express two-phase selection:

```go
type PollingRelayRepository interface {
	ListUnpublished(ctx context.Context, maxEntities, maxEventsPerEntity int) ([]EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}
```

Contract: pick up to `maxEntities` distinct `entity_id`s **at random** from the unpublished
set, return up to `maxEventsPerEntity` events for each, flat, ordered by
`(entity_id, version, index)`. Ordering is the repository's job — the relay groups while
preserving arrival order and does not re-sort.

Postgres, one statement:

```sql
WITH picked AS (
  SELECT DISTINCT entity_id FROM outbox WHERE NOT published ORDER BY random() LIMIT $1
), ranked AS (
  SELECT o.*, row_number() OVER (PARTITION BY o.entity_id ORDER BY o.version, o.idx) rn
  FROM outbox o JOIN picked p USING (entity_id) WHERE NOT o.published
)
SELECT * FROM ranked WHERE rn <= $2 ORDER BY entity_id, version, idx;
```

Mongo takes 1 + K round trips — `$match` + `$group` + `$sample` for the keys, then one
`find` per sampled entity, filtered on that `entity_id`, sorted `(version, idx)`, limited to
`maxEventsPerEntity`. A single `find` across all keys with a shared `maxEntities ×
maxEventsPerEntity` limit was tried first, but the shared budget let one early-sorting,
backlog-heavy entity starve the rest — per-entity fairness is the property this whole
change exists to deliver, so each entity gets its own capped query instead. Avoiding
`$setWindowFields` is deliberate: DocumentDB compatibility is worth the extra queries.

### Config

```go
type PollingRelayConfig struct {
	IdleInterval        time.Duration
	MaxEntitiesPerRound int
	MaxEventsPerEntity  int
	LockKey             string
	Retention           time.Duration
}
```

Defaults ~`100` / `20`. Both validate `> 0` in `validateRelayConfig`. `IdleInterval`,
`LockKey`, and `Retention` keep their names — none is a ceiling, so a `Max` prefix would
misdescribe them, and renaming them widens the break for no gain.

`BatchSize` is gone. Every service constructs via `DefaultPollingRelayConfig(key)`, so the
call sites are unaffected in practice.

### Continuation

`polling_relay.go:160`'s `if published < r.cfg.BatchSize { return }` is meaningless once a
round is entity-scoped — a short round means "these entities had few events", not "the
outbox is empty". It becomes a progress test:

```go
if published == 0 {
	return
}
```

Strictly better: it drains until nothing moves, and it cannot spin when every group's
`sink.Publish` fails, because a round that publishes nothing exits. Per-group failure
handling at `polling_relay.go:104` is unchanged — `continue`, leave unpublished, retry next
round. With random sampling a poison group no longer reliably reappears next round, so it
stops shadowing its neighbours.

### Storage

Mongo (`mongo/event_repository.go:27`) drops `Seq` for two fields:

```go
type entry struct {
	ID       string `bson:"_id"`
	EntityID string `bson:"entity_id"`
	Version  uint64 `bson:"version"`
	Idx      int    `bson:"idx"`
	// type, data, metadata, created_at, published, published_at, expires_at unchanged
}
```

The storage field is `idx`, not `index` — the latter reads badly next to mongo index
management and is a near-keyword in postgres. Same name on both backends. The envelope's
Go field stays `Index`; only persistence abbreviates.

`EnsureOutbox` (`mongo/ensure.go:43`) swaps its partial `seq` index for the drain's access
path:

```go
Keys: bson.D{{Key: "entity_id", Value: 1}, {Key: "version", Value: 1}, {Key: "idx", Value: 1}},
Options: options.Index().SetPartialFilterExpression(bson.D{{Key: "published", Value: false}}),
```

Partial on `published: false` for the reason it exists today — the index tracks the
backlog, not the retained history. It serves phase 2 directly and keeps phase 1's `$group`
scanning backlog-sized data. `$sample` after `$group` runs over the grouped keys in memory
rather than via mongo's optimized random-cursor path, so phase 1 costs one pass over the
distinct unpublished entity ids — bounded by backlog size, near zero in steady state. The
TTL index on `expires_at` is untouched.

Postgres drops `seq bigint` for `version bigint not null` and `idx int not null`, with a
mirroring partial index:

```sql
CREATE INDEX outbox_pending ON outbox (entity_id, version, idx) WHERE NOT published;
```

The table is service-owned, so this is a doc change in the postgres spec plus a migration
per adopting service. The postgres relay is still dormant and no service runs this table
under load, so the migration is cheap now and will not be later.

### WAL is untouched

`postgres/wal` never had a `seq`. Ordering there is WAL commit order, with emit order
inside a transaction — already exactly the invariant this work buys. The one change is
`message.go`: `encode`/`decode` gain `version`/`idx` so the envelope round-trips
losslessly. Nothing reads them on that path, but skipping them would make
`decode(encode(e)) != e`.

The mongo bench fixtures (`mongo/bench_test.go:106`) carry `Seq` and need the same swap.
`embertest.Recorder` is a `Sink` and is unaffected.

## Rollout

Breaking: `PollingRelayRepository`'s signature (the real external break — anyone with a
custom outbox implements it), `PollingRelayConfig` losing `BatchSize`, both outbox schemas,
and foreign-entity events beginning to error. `EventEnvelope`'s two new fields are additive
but visible to `Sink` implementers. `stage`/`build`/`staged` are unexported and do not
count.

**Per-service, not fleet-wide.** Each service owns its own outbox collection or table and
bumps ember on its own schedule. No coordinated cutover, which is what makes a drain
practical.

Legacy rows cannot be backfilled — the outbox never stored a version and the entity has
since moved on — so the migration is a drain rather than a backfill. Per service:

```
stop the service (writers stop; nothing new enters the outbox)
let the running relay finish, or run one final drain
verify: count of {published: false} == 0     <- the gate, not a formality
deploy new ember + call-site changes
start (EnsureOutbox creates the new partial index)
drop the stale seq index                      <- manual
```

The verify step is the whole migration. Nothing ships in the library to support it —
`db.outbox.countDocuments({published: false})` in the runbook is enough. `EnsureOutbox`
stays additive and does not drop the old index; that is a manual ops step.

A missed drain degrades rather than corrupts. Leftover rows have no version, decode as
`version 0`, and sort ahead of everything for their entity — accidentally correct, since
they genuinely are older. Only their order relative to each other is lost. The window is
therefore safe to abort halfway.

**Rollback** is cheap, unlike the `_id` normalisation. Old ember sorts by a `seq` field
that new rows lack; mongo sorts missing fields first, so a reverted binary publishes new
rows before old ones — degraded, not corrupt. Draining again before reverting removes even
that. No snapshot required.

**Bundle it.** conversation-service already has a window planned for its unit-of-work
adoption and the `_id` normalisation. This drain fits inside it — same stop/verify/deploy
shape, and the outbox is empty at that point anyway because the service is stopped.

## Testing

Existing conventions: `testify/suite`, `mock.Mock` doubles, tests in the canonical file for
each unit, doubles from `embertest`.

`saver_test.go`
- Multi-entity, multi-event save: each event carries its own entity's next version, `idx`
  0..n-1 in emit order; an entity at v5 emits at v6. This is the test that catches the
  flatten-into-one-bag problem at `saver.go:40`.
- Foreign event → `ErrForeignEvent`, save aborts, no entity persisted, no version bumped.

`publisher_test.go` — `Publish` stamps `version 0` and `idx` by position.

`polling_relay_test.go`
- Grouping preserves repository order: a double returning a hand-ordered list comes out
  unchanged, proving the relay does not re-sort.
- `published == 0` exits the drain loop; a full round loops again.
- One group's `sink.Publish` failing leaves that group's ids unmarked without stopping the
  others.

`mongo/event_repository_test.go` (integration, real mongo, dialing once per `40bc5d3`) —
where the two-phase query is proven, because it is real-datastore behavior and a fake would
only re-assert our own Go code.
- Seed 3 entities x 5 events, `MaxEntitiesPerRound=2`, `MaxEventsPerEntity=3` → exactly 2
  distinct entities, <=3 events each, each a version-prefix from that entity's lowest
  unpublished version.
- **The regression test for the actual bug:** seed one entity's events with `created_at`
  going backwards, several sharing an identical timestamp, and assert the drain order is
  still strict version order. Fails under `seq = UnixNano`; passes trivially under
  `(version, idx)` because the clock is not in the path.
- Random sampling: over N rounds every seeded entity appears at least once. Coverage, not
  distribution — asserting a distribution invites flake for no information.

`postgres/event_repository_test.go` — sqlmock for the CTE query shape, args, and scan-back
mapping. The repo is dormant; real query behavior gets proven when a service adopts it.

`postgres/wal/message_test.go` — `decode(encode(e)) == e` including `version`/`idx`.

Not tested: mongo's `$sample`, mongo's sort, postgres window functions. Those are the
datastore's tests.

## Out of scope

- **Exposing the version to consumers.** `ReceivedEvent` and the transports are unchanged.
  Revisit when a consumer actually needs stale-redelivery rejection.
- **Backfilling legacy outbox rows.** Replaced by the drain.
- **`entity_type` on the outbox.** Rejected above; revisit only if something starts keying
  idempotency on `(entity_id, version)`.
- **Relay nudge**, **Publisher/Relay split**, and **per-service unit-of-work adoption** —
  separate specs, unaffected by this one.
