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

4. **Deploy** the new ember and the call-site changes.
5. **Start the service.** `EnsureOutbox` creates the new partial index on
   `(entity_id, version, idx)`.
6. **Drop the stale index.** Manual — `EnsureOutbox` is additive and will not
   remove it:

   ```js
   db.outbox.dropIndex("seq_1")
   ```

   On postgres, drop whatever index backed `ORDER BY seq` and create the drain
   index if the service creates its own schema:

   ```sql
   CREATE INDEX outbox_pending ON outbox (entity_id, version, idx) WHERE NOT published;
   ```

## If step 3 is not zero

Leftover rows decode as `version 0` and sort ahead of everything for their
entity — accidentally correct, since they genuinely are older. Only their order
relative to each other is lost. The window is safe to abort halfway; the
leftovers degrade to today's behavior rather than corrupting anything.

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
`ListUnpublished(ctx, maxEntities, maxEventsPerEntity)` signature.

An entity emitting an event whose `EntityID()` is not its own `ID()` now fails
`Save` with `ember.ErrForeignEvent`. Grep for cross-entity `Emit` calls before
deploying.
