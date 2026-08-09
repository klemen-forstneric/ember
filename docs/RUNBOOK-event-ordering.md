# Runbook: ember event ordering migration

Applies to every service running a table- or collection-backed ember outbox with
a `PollingRelay`. Services on `postgres/wal` need only the ember bump — the WAL
path is unchanged.

Legacy outbox rows carry `seq` and no `version`, and the version cannot be
recovered: the outbox never stored it and the entity has moved on since. So the
migration is a drain, not a backfill.

Run this per service. There is no fleet-wide cutover — each service owns its own
outbox and bumps ember on its own schedule.

## Procedure

1. **Stop the service.** All replicas. Writers must stop so nothing new enters
   the outbox.
2. **Let the relay finish**, or run one final drain.
3. **Verify the outbox is empty.** This is the gate, not a formality:

   ```js
   // mongo
   db.outbox.countDocuments({published: false})   // must be 0
   ```

   ```sql
   -- postgres
   SELECT count(*) FROM outbox WHERE NOT published;   -- must be 0
   ```

4. **On postgres, alter the outbox schema before deploying:**

   ```sql
   ALTER TABLE outbox
     ADD COLUMN version bigint NOT NULL DEFAULT 0,
     ADD COLUMN idx     int    NOT NULL DEFAULT 0,
     DROP COLUMN seq;
   ```

   `DEFAULT 0` is load-bearing, not tidiness. Step 3 gates on the outbox being
   empty, but the default is what makes leftover rows sort *ahead* of new events
   for their entity (see "If step 3 is not zero" below) instead of behind them.
   Without it, `version` on any pre-existing row is NULL, and postgres orders
   NULLs last — the exact inverse of what this runbook promises.

   Also create the drain index now. It is required, not optional, and must run
   after the columns above exist:

   ```sql
   CREATE INDEX outbox_pending ON outbox (entity_id, version, idx) WHERE NOT published;
   ```

   Mongo needs no manual schema step here — step 6's `EnsureOutbox` creates the
   new index, and documents simply gain `version`/`idx` fields as new rows land.

5. **Deploy** the new ember and the call-site changes.
6. **Start the service.** On mongo, `EnsureOutbox` creates the new partial index
   on `(entity_id, version, idx)`.
7. **Drop the stale index.** Manual — `EnsureOutbox` is additive and will not
   remove it:

   ```js
   db.outbox.dropIndex("seq_1")
   ```

   Confirm the actual name first with `db.outbox.getIndexes()`. A service whose
   outbox index was created with a custom name gets a no-op `IndexNotFound` from
   `dropIndex("seq_1")`, not a real drop.

   On postgres, drop whatever index backed `ORDER BY seq` — `outbox_pending` was
   already created in step 4.

## If step 3 is not zero

On mongo, leftover rows decode as `version 0` and sort ahead of everything for
their entity — accidentally correct, since they genuinely are older. Only their
order relative to each other is lost. The window is safe to abort halfway; the
leftovers degrade to today's behavior rather than corrupting anything.

On postgres this same guarantee depends entirely on step 4's `DEFAULT 0`. With
it, leftover rows get `version 0` and sort ahead, same as mongo. Without it they
get NULL, which postgres sorts last — leftovers would sort *behind* every new
event for their entity, the opposite of "accidentally correct."

## Rollback

Cheaper than the `_id` normalisation — no snapshot required. Old ember sorts by
a `seq` field that new rows lack, and mongo sorts missing fields first, so a
reverted binary publishes new rows before old ones: degraded, not corrupt.
Draining again before reverting removes even that.

## Bundling

conversation-service already has a window planned for its unit-of-work adoption
and the `_id` normalisation. This drain fits inside it — same stop/verify/deploy
shape, and the outbox is empty at that point anyway because the service is
stopped.

## Call-site changes

`PollingRelayConfig.BatchSize` is gone, replaced by `MaxEntitiesPerRound` and
`MaxEventsPerEntity`. Services constructing config via
`ember.DefaultPollingRelayConfig(key)` need no change. Services setting
`BatchSize` explicitly must set both new fields.

Any service with a custom `PollingRelayRepository` must implement the new
`ListUnpublished(ctx, maxEntities, maxEventsPerEntity)` signature — and that
signature carries a contract the compiler cannot check:

- Pick up to `maxEntities` distinct `entity_id`s **at random** from the
  unpublished set.
- Return up to `maxEventsPerEntity` events for each of those entities.
- The result is flat, grouped by entity, version-ordered within each entity.
  Cross-entity order is unspecified — the relay buckets by `EntityID` before
  publishing, so it never observes it.
- Ordering is the repository's job. The relay does not re-sort what
  `ListUnpublished` returns.

Two implementations compile, satisfy the interface, and pass a naive test
suite, but both starve the outbox under a real backlog:

1. `ORDER BY version, idx LIMIT maxEntities*maxEventsPerEntity` — a global sort
   with no entity sampling first. This reintroduces the starvation the whole
   change exists to remove: a long-lived entity at version 8000 sorts behind
   every version-1 row from every other entity and never drains.
2. `SELECT DISTINCT entity_id ... LIMIT maxEntities` with no `ORDER BY`. This
   returns entities in stable index-scan order, so every round picks the same
   entities and starves the rest. "Unordered" is not "random" — a scan's
   default order is a query-planner artifact, not a shuffle.

An entity emitting an event whose `EntityID()` is not its own `ID()` now fails
`Save` with `ember.ErrForeignEvent`. This fires on a runtime comparison, not a
static pattern, so grepping for `Emit` finds every call site and proves nothing
about which ones are foreign. Instead, run the new ember in staging under
production-shaped traffic and watch for `ErrForeignEvent`. The error is loud
and the save aborts cleanly before the transaction opens: no entity persisted,
no version bumped, and the buffered events are retained for retry.
