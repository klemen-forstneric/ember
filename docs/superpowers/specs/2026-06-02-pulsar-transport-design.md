# Pulsar Publisher and Subscriber transport

**Date:** 2026-06-02
**Status:** Approved design

## Problem

The `pulsar` package is meant to provide the Apache Pulsar implementation of
ember's messaging edges:

- **`pulsar.Subscriber`** should satisfy `ember.Transport` — turn a logical
  subscription name into a live `<-chan ember.AckableEventEnvelope`.
- **`pulsar.Publisher`** should take already-marshaled `ember.EventEnvelope`s and
  push them onto Pulsar topics.

Both are unfinished:

- `pulsar/subscriber.go` does not compile. `Subscribe` references
  `s.consumerFactory`, which is not a field; `NewSubscriber` never wires `cfg`;
  created consumers are never closed (broker-resource leak); the lifecycle method
  is `Shutdown()` while `ember.Transport` requires `Stop()`; and `opt.DLQ` is
  dereferenced without a nil check.
- `pulsar/publisher.go` holds a single `producer`, which is bound to one Pulsar
  topic and therefore cannot route different events to different topics.

This design finishes both and unifies how they obtain Pulsar objects.

## Scope

**In scope:** routing published events to topics by event type; lazy producer
creation and caching; per-subscription consumer fan-in driven by injected config;
clean shutdown and broker-resource release; a testable seam isolating the Pulsar
client.

**Out of scope:** the concrete `*pulsar.Client`-backed registry implementations
(thin edge, integration-test territory); any change to the root `ember` event
model, `Consumer`, or middleware; cross-topic ordering guarantees (Pulsar does not
provide them — see Ordering note).

## Key decisions

1. **Route by event type → topic.** The Publisher maps `e.Event.Type` to a topic
   name via a `map[string]string`. An event whose type has no mapping is a **hard
   error** — `Publish` fails rather than silently dropping a domain fact.

2. **Lazy, cached producers.** Pulsar producers are created on first use for a
   topic and cached (mutex-guarded), rather than eagerly at construction. This
   keeps service startup fast and independent of topic count; the cost is that the
   first publish to each topic pays creation latency and can fail there. Chosen
   over eager creation because this is a library and consumer topic counts vary.

3. **Routing + caching live in a registry, not the Publisher/Subscriber.** The
   "give me the Pulsar object(s) for this logical name" concern — routing table,
   creation, and caching — is folded into a dependency, so the `Publisher` and
   `Subscriber` collapse to thin shells. This also moves the subscriber's config
   (`map[string][]pulsar.ConsumerOptions`) out of the `Subscriber` (where
   `NewSubscriber` never wired it) and into the `consumerRegistry`.

4. **Symmetric `*Registry` naming.** Both sides depend on a registry keyed by a
   logical name, with identical method names:

   ```go
   type producerRegistry interface {
       Get(ctx context.Context, eventType string) (producer, error)
       Close() error
   }

   type consumerRegistry interface {
       Get(ctx context.Context, subscription string) ([]subscriptionConsumer, error)
       Close() error
   }
   ```

   Singular vs. plural return reflects honest cardinality: one topic per event
   type, many topics per subscription.

5. **Keep the per-subscription fan-in.** A subscription maps to a slice of
   `pulsar.ConsumerOptions`; each becomes a Pulsar consumer, and all of them fan
   into the single `out` channel for that subscription. This is kept over a single
   multi-topic consumer (`Topics`/`TopicsPattern`) because the latter applies one
   `DLQ`/`Type`/`KeySharedPolicy` across all topics, whereas the fan-in allows
   **per-topic DLQ/retry/subscription-type** — which the current code already
   relies on (it reads `opt.DLQ.MaxDeliveries` per consumer). A single-topic
   subscription is just a slice of length one.

6. **`Shutdown` → `Stop`.** Rename so `pulsar.Subscriber` actually satisfies
   `ember.Transport`.

## Ordering note

Pulsar guarantees message order only **within a single partition** (and, with
`KeySharedPolicy`, per key within it). There is no cross-topic or cross-partition
ordering. A single multi-topic consumer is fan-in implemented inside the client,
so it offers the *same* guarantee as the application-level fan-in here — neither
orders across topics. Per-entity ordering in this system therefore depends on
topic layout: it holds only while a given entity's events all flow through one
topic (the producer keys messages by `EntityID`, landing them on one partition,
and `ember.StickyEntityConsumer` hashes `EntityID` to a worker). Splitting one
entity's events across topics breaks total per-entity order regardless of the
consumer shape.

## Components

### `producer` / `consumer` (unchanged low-level interfaces)

The existing narrow interfaces that wrap the Pulsar SDK objects:

```go
type producer interface {
    SendAsync(context.Context, *pulsar.ProducerMessage,
        func(pulsar.MessageID, *pulsar.ProducerMessage, error))
    Close()
}

type consumer interface {
    Chan() <-chan pulsar.ConsumerMessage
    Ack(pulsar.Message) error
    Nack(pulsar.Message)
    Close()
}
```

`Close()` is added to both so registries can release them. It is **void**, not
`error`, so the interfaces remain directly satisfiable by `pulsar.Producer` /
`pulsar.Consumer` (whose `Close()` are void) without an adapter. The registries'
`Close() error` therefore aggregates nothing from the SDK and returns `nil` in
the real impls; the `error` return is kept for a uniform closer signature.

### `subscriptionConsumer`

Carries what the subscriber needs from each created consumer without leaking the
full `pulsar.ConsumerOptions` past the registry boundary:

```go
type subscriptionConsumer struct {
    consumer      consumer
    maxDeliveries int // derived from opts.DLQ; 0 when DLQ is nil
}
```

The registry computes `maxDeliveries` and guards the `opt.DLQ` nil case there.

### `producerRegistry` (real impl)

Constructed with the route table and a `*pulsar.Client`. `Get(ctx, eventType)`:

1. `topic, ok := routes[eventType]`; `!ok` → return an "unmapped event type" error.
2. Under a `sync.Mutex`, return the cached producer for `topic`, or create it via
   the client, cache it, and return it.

`Close()` closes every cached producer, joining errors.

### `consumerRegistry` (real impl)

Constructed with `map[string][]pulsar.ConsumerOptions` and a `*pulsar.Client`.
`Get(ctx, subscription)`:

1. `opts, ok := config[subscription]`; `!ok` → return an "unknown subscription"
   error.
2. For each option, create a consumer and wrap it in a `subscriptionConsumer`
   (computing `maxDeliveries` with a nil-`DLQ` guard). On any creation failure,
   close those already created and return the error.

`Close()` closes every consumer it created, joining errors.

### `pulsar.Publisher`

```go
type Publisher struct {
    registry producerRegistry
}

func NewPublisher(r producerRegistry) *Publisher
```

`Publish(ctx, ...EventEnvelope)`:

1. Return early on empty input.
2. For each envelope: extract `correlation_id` from metadata (existing hard error
   if missing); `p.registry.Get(ctx, e.Event.Type)` (error → return it); marshal
   to the existing `message` shape; `SendAsync` keyed by `EntityID` with
   `EventTime` set.
3. Aggregate async send errors via `errors.Join` (existing pattern).

`Close()` delegates to `registry.Close()`.

### `pulsar.Subscriber`

```go
type Subscriber struct {
    registry consumerRegistry
    logger   ember.LoggerCtx
    shutdown chan struct{}
    wg       sync.WaitGroup
}

func NewSubscriber(r consumerRegistry, l ember.LoggerCtx) *Subscriber
```

`Subscribe(ctx, name) (<-chan ember.AckableEventEnvelope, error)`:

1. `scs, err := s.registry.Get(ctx, name)`; on error return it.
2. Create one `out` channel for this call.
3. For each `subscriptionConsumer`, spawn a goroutine (`s.wg.Add(1)`) running the
   existing loop: read `consumer.Chan()`, unmarshal `message` (on failure log and
   `continue`), rebuild `Metadata` stamping `current_delivery`
   (`msg.RedeliveryCount()`), `max_deliveries` (`sc.maxDeliveries`), and
   `correlation_id`, build the `AckableEventEnvelope` with `Ack`/`Nack` closures,
   and forward to `out` — bailing on `<-s.shutdown`.
4. Return `out`.

`Stop()`:

```go
func (s *Subscriber) Stop() {
    close(s.shutdown)
    s.wg.Wait()                       // forwarding goroutines drained
    if err := s.registry.Close(); err != nil {
        s.logger.Error(context.Background(), "Could not close consumer registry", err)
    }
}
```

`out` is intentionally **not** closed: the downstream `ember.Consumer` (`Serial`/
`StickyEntity`) terminates via its own `Stop()`/`ctx`, and `ember.Subscriber.Stop`
calls both `transport.Stop()` and `consumer.Stop()`. Closing `out` would add a
closer goroutine and tracking state for no behavioral benefit. Broker resources
are released by `registry.Close()` after `wg.Wait()`, so no goroutine is still
reading a consumer when it is closed.

## Error handling

| Situation | Behavior |
|---|---|
| Publish: event type not in routes | `registry.Get` errors → `Publish` returns it (no send) |
| Publish: producer creation fails | surfaces from `registry.Get` → returned from `Publish` |
| Publish: async send failures | collected via `errors.Join` (existing) |
| Publish: missing `correlation_id` | hard error (existing) |
| Subscribe: unknown subscription name | `registry.Get` errors → `Subscribe` returns it |
| Subscribe: malformed Pulsar payload | log + `continue` (do not ack a bad frame) |
| `opt.DLQ` nil | guarded in registry; `maxDeliveries = 0` |
| `registry.Close()` | closes all SDK objects (void `Close()`); returns `nil`; `Stop` logs the (nil) result |

## Testing

Mirrors the package's existing fake-interface style; no real broker required. The
`producerRegistry`/`consumerRegistry` and `producer`/`consumer` interfaces are all
mockable.

- **Publisher:** routes to the correct producer; unmapped-type error; lazy
  create-once (second publish to same topic hits cache); `errors.Join`
  aggregation across messages; `Close` closes cached producers.
- **Subscriber:** fan-in (N consumers → one channel); metadata stamping
  (`current_delivery`/`max_deliveries`/`correlation_id`); ack on handler success,
  nack on handler error; unmarshal-skip path; `Stop()` drains goroutines and
  closes the registry.
- **Out of scope:** the `*pulsar.Client`-backed registry implementations are the
  thin untested edge (integration tests).

## Extensibility

- Strict all-or-nothing publish batches would require a pure `Topic(eventType)
  (string, bool)` lookup on `producerRegistry` and a pre-scan before sending;
  deferred as likely unnecessary given the lazy-creation choice.
- An eager / background-prewarm producer strategy can be added as an alternative
  `producerRegistry` implementation without touching the `Publisher`.
