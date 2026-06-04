# Migrate ember tests to testify (mocks + suites)

**Date:** 2026-06-04
**Status:** Approved

## Goal

Rewrite the library's tests to use [testify](https://github.com/stretchr/testify):
assertions, test suites where there is shared setup, and — the primary driver —
hand-written `testify/mock` mocks that replace the bespoke `fake*` / `stub*`
implementations currently scattered across the test files.

## Background

Today every package rolls its own test doubles:

- `kafka/fakes_test.go` — `fakeWriter`, `fakeReader`, `fakeConsumerRegistry`
- `pulsar/fakes_test.go` — `fakeProducer`, `fakeProducerRegistry`, `fakeConsumer`,
  `fakeConsumerRegistry`
- `entity_test.go` (root) — `fakeRepo`, `fakeMarshaler`, `failingMarshaler`,
  `fakeEntity`
- `pulsar/subscriber_test.go` — `stubMessage` (minimal `pulsar.Message`)

Assertions are hand-rolled (`if got != want { t.Fatalf(...) }`). There are no
test dependencies today (no testify, no codegen tooling).

Module: `github.com/klemen-forstneric/ember`, Go 1.26.3.

## Decisions

These were settled during brainstorming:

1. **Mocks are hand-written `testify/mock`** — no codegen (no mockery/gomock).
   Standard idiom: embed `mock.Mock`, configure with `On(...).Return(...)`.
2. **Suites only where there is real shared setup.** Pure / table-driven tests
   stay plain functions and just adopt testify assertions.
3. **Mock only genuine behavioral collaborators.** Keep `fakeEntity` as a plain
   test struct and keep `stubMessage` minimal — turning a 15-method
   `pulsar.Message` into a full mock is noise.
4. **`require` vs `assert`:** `require` where a failure should stop the test
   (unexpected errors, nil checks that later lines depend on); `assert` for the
   remaining value checks.

## Approach

### Dependency

Add `github.com/stretchr/testify` (latest) to `go.mod`.

### Mocks (hand-written, one `mocks_test.go` per package)

Replace `fakes_test.go` / inline fakes with mocks embedding `mock.Mock`. Mocks
live in `_test.go` files — the transport interfaces are package-private, so the
mocks are not exported for library consumers.

| Package | Mock(s) | Interface replaced |
|---|---|---|
| `kafka`  | `mockWriter`, `mockReader`, `mockConsumerRegistry` | `writer`, `reader`, `consumerRegistry` |
| `pulsar` | `mockProducer`, `mockProducerRegistry`, `mockConsumer`, `mockConsumerRegistry` | `producer`, `producerRegistry`, `consumer`, `consumerRegistry` |
| root     | `mockEntityRepository`, `mockEntityMarshaler` | `EntityRepository`, `EntityMarshaler[E]` |

`fakeEntity` stays a plain struct. `stubMessage` stays a minimal
`pulsar.Message` (or a small constructor helper).

#### Streaming-interface wrinkle

Several interfaces have blocking / channel-returning methods that pure
`On(...).Return(...)` cannot model:

- `reader.FetchMessage` — blocks until a message is available.
- `consumer.Chan()` — returns a `<-chan ConsumerMessage`.

For these, the mock is **a testify `mock.Mock` plus a channel field**: the test
sets up the channel to drive delivery while `mock.Mock` still records calls and
configures the non-streaming methods (`Ack`/`Nack`/`CommitMessages`/`Close`/
`MaxDeliveries`/`RetryBackoff`). This is the accepted testify pattern for
streaming collaborators; it is expected and not a smell.

### Suites (`testify/suite`)

Use suites where each test shares setup (fresh mocks + subject in `SetupTest`):

- `kafka`: publisher, subscriber, consumer_registry
- `pulsar`: publisher, subscriber
- root: entity store

Stay plain functions with testify assertions (no suite):

- `filter_test.go` (root, dynamo, postgres, mongo)
- `dynamo/data_test.go`
- `kafka/message_test.go`
- `kafka/offset_tracker_test.go`

### Behavior preservation

Each migrated test keeps the same scenarios it covers today (routing, batching,
error aggregation, offset tracking, contiguous commits, retries, ack/nack,
fan-in, marshaler failure paths, repository error paths). Only the mechanism
changes (testify assertions + mocks), not the coverage.

## Testing / Verification

- After each package migration: `go test ./<pkg>/...` passes. Mocks mean no
  Kafka / Pulsar / DynamoDB / Postgres / Mongo / Redis infrastructure is needed.
- Final: `go test ./...`, `go vet ./...`, and `gofmt -l` all clean.

## Out of scope

- No codegen tooling.
- No new behavioral coverage beyond what exists today.
- No changes to non-test (production) source files, except adding testify to
  `go.mod`/`go.sum`.
- No unrelated refactoring.
