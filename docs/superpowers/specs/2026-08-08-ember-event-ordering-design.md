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

**Fetch order sorts by `(entity_id, version, idx)`, not `(version, idx)` alone.**
`ORDER BY version, idx` as a *global* sort is correct — within an entity v1 always precedes
v2, so any prefix of that sort is a valid prefix per entity — but it starves. An entity at
version 8000 sorts behind every version-1 event in the outbox, and under a backlog, freshly
created entities keep arriving at v1 and keep jumping the queue. The high-version entity is
not merely delayed; it can starve indefinitely, and arrivals at v1 are unbounded so it never
reaches the front. That bites exactly the long-lived, high-churn entities that matter most,
and it is the exact failure the original two-phase random-sample design existed to prevent.

Putting `entity_id` first removes that failure mode without a second phase. A single query,
sorted `(entity_id, version, idx)` with one `LIMIT`, is a prefix of a correctly-sorted set:
per entity, that prefix is a valid version-ordered run, because sorting by `entity_id` first
cannot interleave one entity's versions with another's out of order. The trade this makes
honestly: low-`entity_id` entities win the window every round, and an entity can still starve
if earlier-sorting entities keep producing faster than the relay drains them. But that
requires *sustained saturation* — the system already losing regardless of drain strategy —
not merely a healthy backlog with new entities arriving, which is what broke the global
`(version, idx)` sort. As a sorted prefix's events publish and get marked, they leave the
unpublished set and the window advances past them; the starvation is bounded and self-healing
rather than the original failure mode's unbounded, permanent kind.

A two-phase design (mongo: `$group`/`$sample` for entity ids, then one `find` per sampled
entity; postgres: a `picked`/`ranked` CTE with `row_number()` per entity) was built first to
avoid this trade entirely via random entity selection. It was reverted: it costs 1 + K round
trips per round on mongo and a window-function CTE on postgres, and the repo owner judged
that cost not worth buying out a failure mode that only shows up under sustained saturation.

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

`PollingRelayRepository.ListUnpublished(ctx, limit)` is one query, one round trip:

```go
type PollingRelayRepository interface {
	ListUnpublished(ctx context.Context, limit int) ([]EventEnvelope, error)
	MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error
}
```

Contract: return up to `limit` unpublished events, flat, ordered by `(entity_id, version,
idx)`. Ordering is the repository's job — the relay buckets by `EntityID` before publishing
and does not re-sort, so it only ever observes each entity's slice, which is guaranteed a
valid version-ordered prefix by the sort itself.

A non-positive `limit` is `ErrInvalidLimit`, not an unlimited fetch. The relay always
passes a `BatchSize` that `validateRelayConfig` has already rejected unless positive, so
zero can only arrive from a direct caller — and there it would load the entire backlog into
memory. Mongo makes that failure especially quiet, since `SetLimit(0)` also means
unlimited; omitting the call and passing zero are the same thing, so neither can be the
signal.

Postgres, one statement, built with squirrel like `Save` and `MarkPublished`:

```sql
SELECT id, entity_id, type, data, metadata, version, idx, created_at
FROM outbox WHERE NOT published
ORDER BY entity_id, version, idx
LIMIT 500;
```

`WHERE NOT published` is a bare predicate (`sq.Where("NOT published")`), not
`sq.Eq{"published": false}` — the latter binds `published = $1`, which a generic prepared-
statement plan cannot prove implies the partial index's `WHERE NOT published` predicate, so
the index silently stops being used. `LIMIT` is squirrel's `.Limit(n)`, which inlines the
value as a literal rather than a bind parameter, so it is never a `$n` placeholder either.

Mongo is a single `find`: filter `{published: false}`, sort `{entity_id: 1, version: 1, idx:
1}`, limit `limit`. Both backends dropped the two-phase machinery this replaced — mongo's
`$group`/`$sample` entity sample plus one `find` per sampled entity, and postgres's
`picked`/`ranked` CTE with a `row_number()` window function partitioned per entity. Both
existed solely to buy random entity selection, which bought out the starvation trade this
design now accepts (see the "Fetch order" decision above) at a cost of 1 + K round trips per
round on mongo and a window function on postgres. The repo owner judged that not worth it.

### Config

```go
type PollingRelayConfig struct {
	IdleInterval time.Duration
	BatchSize    int
	LockKey      string
	Retention    time.Duration
}
```

Back to the original single knob: `BatchSize` replaces `MaxEntitiesPerRound` and
`MaxEventsPerEntity`, default `500`, validated `> 0` in `validateRelayConfig` like the others.
`IdleInterval`, `LockKey`, and `Retention` are unaffected.

### Continuation

`published == 0` (`polling_relay.go`'s `tick`) is still the right progress test with a flat,
single-query fetch — it drains until a round moves nothing, and it cannot spin when every
group's `sink.Publish` fails, because that round publishes zero and exits. Per-group failure
handling in `publish` is unchanged — `continue`, leave unpublished, retry next round. A
failing group is refetched at the front of the next round (lowest `entity_id`), so once the
batch is nothing but failing rows, the round publishes zero and `tick` exits rather than
spinning on the same poison group forever.

That non-spin does not mean the round is harmless. A single permanently-failing entity
holding the lowest `entity_id` with at least `BatchSize` unpublished events occupies the
entire window every round: every fetch returns only that entity's events, `publish` returns
0, and `tick` exits without anyone else draining — head-of-line blocking, no saturation
required, with no upper bound on how long it lasts. The random-sample design this replaced
made a poison group not reliably reappear next round; the entity-first sort trades that away
for the simpler query. Accepted, not new — it is the pre-branch outbox's behavior returning
under the new key.

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
backlog, not the retained history. It serves `ListUnpublished`'s find directly: filter and
sort both hit the index, so the query is an index scan bounded by `limit`, not a scan of the
backlog. The TTL index on `expires_at` is untouched.

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
custom outbox implements it), `PollingRelayConfig`'s shape (`BatchSize` again, single-knob),
both outbox schemas, and foreign-entity events beginning to error. `EventEnvelope`'s two new
fields are additive but visible to `Sink` implementers. `stage`/`build`/`staged` are
unexported and do not count.

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
where the sorted find is proven, because it is real-datastore behavior and a fake would only
re-assert our own Go code.
- One entity's events ordered by version then idx.
- **The regression test for the actual bug:** seed one entity's events with `created_at`
  going backwards, several sharing an identical timestamp, and assert the drain order is
  still strict version order. Fails under `seq = UnixNano`; passes trivially under
  `(version, idx)` because the clock is not in the path.
- Several entities seeded out of order: the flat result is ordered by `(entity_id, version,
  idx)`, and each entity's run within it is a version-ordered prefix.
- `ListUnpublished` respects `limit`; `MarkPublished` drops an event out of the pending set.

`postgres/event_repository_test.go` — sqlmock for the plain `SELECT ... ORDER BY entity_id,
version, idx LIMIT $n` query shape, args, and scan-back mapping. The repo is dormant; real
query behavior gets proven when a service adopts it.

`postgres/wal/message_test.go` — `decode(encode(e)) == e` including `version`/`idx`.

Not tested: mongo's sort, postgres's `ORDER BY`/`LIMIT` execution. Those are the datastore's
tests.

## Out of scope

- **Exposing the version to consumers.** `ReceivedEvent` and the transports are unchanged.
  Revisit when a consumer actually needs stale-redelivery rejection.
- **Backfilling legacy outbox rows.** Replaced by the drain.
- **`entity_type` on the outbox.** Rejected above; revisit only if something starts keying
  idempotency on `(entity_id, version)`.
- **Relay nudge**, **Publisher/Relay split**, and **per-service unit-of-work adoption** —
  separate specs, unaffected by this one.
