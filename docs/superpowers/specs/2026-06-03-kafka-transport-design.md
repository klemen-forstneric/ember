# Kafka Publisher and Subscriber transport

**Date:** 2026-06-03
**Status:** Approved design

## Problem

ember needs an Apache Kafka implementation of its messaging edges, alongside the
existing Pulsar one:

- **`kafka.Subscriber`** should satisfy `ember.Transport` — turn a logical
  subscription name into a live `<-chan ember.AckableEventEnvelope`.
- **`kafka.Publisher`** should take already-marshaled `ember.EventEnvelope`s and
  route them onto Kafka topics by event type.

The goal is structural familiarity with the `pulsar` package — same wire envelope,
same metadata keys, thin `Publisher`/`Subscriber` shells over narrow SDK
interfaces — while honestly handling the places where Kafka's delivery model
differs from Pulsar's. The client library is `github.com/segmentio/kafka-go`
(pure Go, keeps ember CGo-free; there is no official Apache Kafka Go client).

## The core difference from Pulsar

**Pulsar's unit of acknowledgment is the individual message; Kafka's is the
partition offset — a single cumulative cursor.**

In Pulsar (Shared / Key_Shared), the broker tracks ack state per message:
`Ack(msg)` acks that specific message in any order, and `Nack(msg)` redelivers
that one message after a delay. ember's per-message `Ack()`/`Nack()` closures map
onto Pulsar for free because the broker does the bookkeeping.

Kafka offers none of that. A consumer-group member's progress in a partition is a
single committed offset meaning "everything up to here is done." `CommitMessages`
is **cumulative** (committing offset N marks all offsets ≤ N done), there is **no
per-message redelivery**, and there is **no native delivery-attempt counter**
(unlike Pulsar's `RedeliveryCount()`). A kafka-go `Message` carries only
`Topic`, `Partition`, `Offset`, `Key`, `Value`, `Headers`, `Time`.

Consequently, to honor ember's per-message `Ack()`/`Nack()` contract on Kafka,
**the transport itself must own the bookkeeping the Pulsar broker does** — see the
offset/retry engine below. This is a baseline correctness requirement, not an
optional feature: it is the only way the transport behaves identically regardless
of which downstream `ember.Consumer` (`SerialConsumer`, `StickyEntityConsumer`,
…) is wired in.

## Scope

**In scope:** routing published events to topics by event type via a single
multi-topic writer; per-subscription consumer-group readers; an offset/retry
engine that makes per-message `Ack`/`Nack` correct under any downstream consumer
and under consumer-group rebalances; a per-subscription delivery cap so a poison
message cannot stall a partition; clean shutdown and resource release; testable
seams isolating the Kafka client.

**Out of scope:** the concrete `*kafka.Writer` / `*kafka.Reader`-backed edges
(thin, integration-test territory); a dead-letter topic on cap-exhaustion (a
message that exhausts its attempts is dropped+logged — DLQ deferred); a
cross-restart-durable attempt counter (the counter is per-session — see Delivery
attempts); any change to the root `ember` event model, `Consumer`, or middleware.

## Key decisions

1. **Route by event type → topic.** The Publisher maps `e.Event.Type` to a topic
   name via a `map[string]string`. An unmapped event type is a **hard error** —
   `Publish` fails rather than silently dropping a domain fact. Mirrors Pulsar.

2. **One shared multi-topic writer, no `producerRegistry`.** A single
   topic-less `*kafka.Writer` routes per-message by setting `Message.Topic`, and is
   goroutine-safe. Kafka has no per-topic producer object to lazily create and
   cache, so a producer registry (which on the Pulsar side exists precisely to
   cache per-topic producers) would be empty ceremony. The `Publisher` holds the
   `writer` and the route table directly. This is a deliberate asymmetry with the
   Pulsar package, justified by the SDK's shape.

3. **One reader per subscription, driven by a consumer group.** A `kafka.Reader`
   with `GroupID` set is the consumer-group member: it joins the group, receives a
   partition assignment, and rebalances automatically. A single reader handles
   multi-topic, multi-partition assignment inside one member, so the Pulsar-style
   "one consumer per topic, fan in" is unnecessary. The `consumerRegistry` returns
   a **single `reader`** per subscription (honest cardinality), and the
   `Subscriber` runs one session (a fetch loop + a retry loop) per subscription —
   no fan-in.

4. **Horizontal scaling is the consumer group.** Run N ember replicas (e.g. k8s)
   with the **same `GroupID`** per subscription; Kafka distributes the
   subscription's partitions across them with no extra code. Scale throughput by
   adding replicas (up to the partition count) or adding partitions. `GroupID` is
   per-subscription config, defaulting to the subscription name when empty.

5. **The Subscriber owns an offset/retry engine.** Per partition it tracks
   delivery attempts, finished offsets, and a commit watermark, committing only the
   highest *contiguous* finished offset (see Offset/retry engine). This makes
   cumulative commits safe under out-of-order completion (e.g.
   `StickyEntityConsumer`) and under rebalances.

6. **Per-subscription delivery cap; poison messages are dropped, not stalled.**
   `MaxDeliveries` is per-subscription config. A nacked message is retried
   in-session up to the cap; on the cap it is **dropped + logged** and its offset
   committed, so it never wedges the partition for the whole group. (DLQ on
   exhaustion is deferred.) With no cap configured, retries are unbounded
   in-session (mirrors Pulsar's no-DLQ path) — documented poison-stall risk.

7. **Same wire envelope and metadata keys as Pulsar.** The Kafka payload is the
   same `message` JSON struct; `Key = EntityID`; the same metadata keys
   (`correlation_id`, `current_delivery`, `max_deliveries`) are stamped with the
   same meaning, so downstream code and middleware are transport-agnostic. The
   `message` struct and keys are duplicated in the `kafka` package (as the
   `pulsar` package has its own unexported copies).

## Delivery attempts and the per-session counter

Kafka has no attempt counter, so the transport maintains one in memory, keyed by
offset within each owning member. Because a single message cannot be redelivered
by Kafka, a `Nack()` does **not** touch Kafka: the transport re-emits the same
envelope onto the `out` channel (after a backoff) and increments the counter. This
provides both in-session retries and the count.

`current_delivery` = the attempt number (1-based); `max_deliveries` = the cap,
stamped **iff** the subscription is capped — identical to Pulsar's metadata
(`RedeliveryCount()+1` / `MaxDeliveries()`).

The counter is **per-session / per-member**: on a rebalance or restart, an
uncommitted offset is redelivered from the last commit and its counter resets to
1. The cap is therefore "attempts per owning member," not "attempts ever." This is
the standard Kafka approach and is acceptable under at-least-once with idempotent
handlers. A cross-restart-durable counter would require carrying the count in a
message header and republishing on each retry (a retry-topic hop) — deferred.

## Ordering note

Kafka guarantees order only **within a single partition**. There is no cross-topic
or cross-partition ordering. Per-entity ordering therefore holds only while a given
entity's events all flow through one topic: the writer keys messages by `EntityID`,
landing them on one partition (ordered), and `ember.StickyEntityConsumer` hashes
`EntityID` to one worker (serial per entity). Splitting one entity's events across
topics breaks total per-entity order. Same shape as the Pulsar transport.

## Components

### `writer` (narrow interface over `*kafka.Writer`)

```go
type writer interface {
    WriteMessages(ctx context.Context, msgs ...kafka.Message) error
    Close() error
}
```

`*kafka.Writer` satisfies this directly. The writer is constructed topic-less
(`Topic` unset) so each `kafka.Message` carries its own `Topic`.

### `kafka.Publisher`

```go
type Publisher struct {
    w      writer
    routes map[string]string // eventType -> topic
}

func NewPublisher(w writer, routes map[string]string) *Publisher
```

`Publish(ctx, ...EventEnvelope)`:

1. Return early on empty input.
2. For each envelope: extract `correlation_id` from metadata (hard error if
   missing, existing behavior); look up `routes[e.Event.Type]` (unmapped → hard
   error); marshal the `message` JSON; build a `kafka.Message{Topic, Key:
   EntityID, Value: payload, Time: e.Timestamp}`.
3. One `w.WriteMessages(ctx, all...)` call; return its error. (kafka-go's
   `WriteMessages` is synchronous and accepts a multi-topic batch, so no
   `errors.Join`-over-async-callbacks is needed as on the Pulsar side.)

`Close()` delegates to `w.Close()`.

### `reader` (narrow interface) and `kafkaReader` adapter

```go
type reader interface {
    FetchMessage(ctx context.Context) (kafka.Message, error)
    CommitMessages(ctx context.Context, msgs ...kafka.Message) error
    Close() error
    MaxDeliveries() (int, bool)   // (cap, capped) — same shape as the Pulsar consumer
    RetryBackoff() time.Duration  // per-subscription re-emit delay on Nack
}
```

A raw `*kafka.Reader` has no `MaxDeliveries()`/`RetryBackoff()`, so the registry
wraps it in a `kafkaReader` adapter (embeds `*kafka.Reader`, promoting
`FetchMessage`/`CommitMessages`/`Close`, and adds the config-derived
`MaxDeliveries` and `RetryBackoff`). `capped=false` means no cap → unbounded
in-session retry and `max_deliveries` omitted (mirrors Pulsar's no-DLQ consumer).
Both cap and backoff are per-subscription config, exposed here so the Subscriber's
engine reads them off the reader without depending on the registry's config type.

### `consumerRegistry` (interface) and `ConsumerRegistry` (real impl)

```go
type consumerRegistry interface {
    Get(ctx context.Context, subscription string) (reader, error)
    Close() error
}

type SubscriptionConfig struct {
    GroupID       string        // defaults to the subscription name when empty
    Topics        []string      // one or more topics for this subscription
    MaxDeliveries int           // 0/absent => uncapped (capped=false)
    RetryBackoff  time.Duration // re-emit delay on Nack; 0 => small default
}
```

`ConsumerRegistry` is constructed with `map[string]SubscriptionConfig`, the broker
addresses, and any shared reader options. `Get(subscription)`:

1. `cfg, ok := config[subscription]`; `!ok` → "unknown subscription" error.
2. Build one `kafka.Reader` with `GroupID` (or subscription name) and the topics
   (`GroupTopics` when several, `Topic` when one), wrap it in a `kafkaReader`
   carrying `(MaxDeliveries, capped)` and the `RetryBackoff` (substituting the
   small default when zero), record it for teardown, return it.

`Close()` closes every reader it created.

### `kafka.Subscriber` and the offset/retry engine

```go
var _ ember.Transport = (*Subscriber)(nil)

type Subscriber struct {
    registry consumerRegistry
    logger   ember.LoggerCtx
    shutdown chan struct{}
    wg       sync.WaitGroup
}

func NewSubscriber(r consumerRegistry, l ember.LoggerCtx) *Subscriber
```

The retry backoff is per-subscription and read off the reader
(`reader.RetryBackoff()`), so the constructor matches the Pulsar
`NewSubscriber(r, l)` signature.

`Subscribe(ctx, name) (<-chan ember.AckableEventEnvelope, error)`:

1. `r, err := s.registry.Get(ctx, name)`; on error return it.
2. Create one `out` channel and start a per-subscription **session** with two
   goroutines — a **fetch loop** and a single **retry loop** — fanning into `out`.
3. Return `out`.

The session keeps **per-partition state** in an `offsetTracker`, mutex-guarded:

- `attempts map[int64]int` — delivery count per offset,
- `done map[int64]bool` — offsets that are finished (acked **or** given-up),
- a per-partition `cursor` watermark (lowest uncommitted offset; commits advance
  it across the contiguous run of `done` offsets).

It also holds an in-memory **retry queue** (`[]retryMessage{m, readyAt}`,
mutex-guarded) plus a buffered(1) wakeup channel.

Behavior:

- **Fetch loop:** `FetchMessage` → register the offset (`attempts=1`) → `deliver`.
- **`deliver`:** unmarshal the `message` (on failure: log, mark the offset `done`,
  and commit past it — a malformed frame is poison and will be malformed on every
  redelivery, so dropping it is the only way to avoid stalling the partition) →
  stamp metadata (`correlation_id`, `current_delivery = attempts[offset]`, and
  `max_deliveries` iff `capped`) → build the `AckableEventEnvelope` with
  `Ack`/`Nack` closures capturing the `kafka.Message` → forward to `out`, bailing
  on `<-ctx.Done()`.
- **Ack(offset):** mark `done`; advance the watermark across the contiguous run of
  `done` offsets; if it advanced, `CommitMessages` the highest contiguous message.
- **Nack(offset), under cap (or uncapped):** `attempts++` and **append the message
  to the retry queue** with `readyAt = now + backoff`, then send a non-blocking
  wakeup. Enqueue is **non-blocking** — it must never block the downstream worker,
  because the retry loop re-delivers through the same `out` and would otherwise
  deadlock waiting on a worker stuck in `Nack`.
- **Retry loop:** waits for the head message's `readyAt` (backoff is constant per
  reader, so the FIFO queue is `readyAt`-ordered), then `attempts++` via the
  tracker and re-`deliver`s it. One retry loop per reader — not a goroutine +
  timer per nacked message.
- **Nack(offset), cap reached:** **give up** — log + drop, mark `done`, advance
  watermark/commit so the partition keeps moving.

Both goroutines are registered on the Subscriber's `wg` in `Subscribe` (before any
`Stop`/`Wait`), and `Nack` never touches `wg` — so no `wg.Add` can race `wg.Wait`,
and the lifecycle needs no separate "stopped" guard.

`Stop()`:

```go
func (s *Subscriber) Stop() {
    s.cancel()  // unblocks FetchMessage, retry-loop waits, and out-sends
    s.wg.Wait() // fetch + retry loops of every session drained
    if err := s.registry.Close(); err != nil {
        s.logger.Error(context.Background(), "Could not close consumer registry", err)
    }
}
```

`out` is intentionally **not** closed (same reasoning as Pulsar: downstream
`ember.Consumer` terminates via its own `Stop()`/`ctx`, and `ember.Subscriber.Stop`
calls both `transport.Stop()` and `consumer.Stop()`). Readers are closed by
`registry.Close()` after `wg.Wait()`.

## Consumer-group rebalance behavior

The per-partition state is per-member and in-memory. On a rebalance that revokes a
partition from this replica:

- Offsets held uncommitted (in-flight or mid-retry) are simply not committed, so
  the partition's new owner redelivers them from the last committed offset.
  **At-least-once is preserved**; idempotent handlers absorb the reprocessing.
- The in-memory attempt counter is discarded; the new owner starts each
  redelivered offset at attempt 1 (per-member cap, as above).
- `CommitMessages` against a just-revoked partition may error; the manager logs it
  and continues.

## Error handling

| Situation | Behavior |
|---|---|
| Publish: event type not in routes | hard error → `Publish` returns it (no send) |
| Publish: missing `correlation_id` | hard error (existing) |
| Publish: `WriteMessages` fails | returned from `Publish` |
| Subscribe: unknown subscription name | `registry.Get` errors → `Subscribe` returns it |
| Subscribe: malformed Kafka payload | log + drop: mark done, commit past it (poison frame; never deliverable, so it must not stall the partition) |
| Nack under cap | enqueue to the retry queue (non-blocking), `attempts++`; retry loop re-emits after backoff |
| Nack at cap | drop + log, mark done, commit past — partition not stalled |
| No cap configured | unbounded in-session retry; `max_deliveries` omitted; poison-stall risk documented |
| `CommitMessages` after partition revoked | log + continue |
| `registry.Close()` | closes all readers; `Stop` logs any error |

## Testing

Mirrors the `pulsar` package's fake-interface style; no real broker required. The
`writer`, `reader`, and `consumerRegistry` interfaces are all mockable.

- **Publisher:** routes to the correct topic; unmapped-type error; multi-topic
  batch in one `WriteMessages`; missing-`correlation_id` error; `Close` closes the
  writer.
- **Subscriber / offset engine:**
  - fetch → forward → ack → commit of the right offset;
  - **contiguous-commit correctness under out-of-order acks** (ack offset 5 before
    3 commits nothing past 2; acking 3 then jumps the commit to 5) — the key new
    test that proves consumer-independence;
  - retry-then-ack (`current_delivery` increments, eventual commit);
  - cap reached → give-up → commit-past (poison message does not stall);
  - metadata stamping (`current_delivery`/`max_deliveries`/`correlation_id`);
  - malformed-payload path: dropped + committed past (does not stall);
  - `Stop()` drains fetch + retry goroutines and closes the registry.
- **Out of scope:** the `*kafka.Writer` / `*kafka.Reader`-backed edges are the thin
  untested boundary (integration tests).

## Extensibility

- A dead-letter topic on cap-exhaustion can be added at the give-up point
  (republish + commit) without touching the engine's structure.
- A cross-restart-durable attempt counter can be layered via a retry-topic hop
  carrying the count in a header, again localized to the Nack path.
- Per-subscription retry backoff is a single knob today (fixed delay); the repo
  already vendors `cenkalti/backoff` if an exponential policy is wanted later.
