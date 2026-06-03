package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/segmentio/kafka-go"
)

// Subscriber is the Kafka implementation of ember.Transport. It resolves a
// subscription to one consumer-group reader and runs an offset/retry engine
// that makes per-message Ack/Nack correct under any downstream consumer.
var _ ember.Transport = (*Subscriber)(nil)

// consumerRegistry resolves the reader for a subscription name. Get returns an
// error for an unknown subscription.
type consumerRegistry interface {
	Get(ctx context.Context, subscription string) (reader, error)
	Close() error
}

type Subscriber struct {
	registry consumerRegistry
	logger   ember.LoggerCtx

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewSubscriber(r consumerRegistry, l ember.LoggerCtx) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())
	return &Subscriber{registry: r, logger: l, ctx: ctx, cancel: cancel}
}

// Subscribe ignores the caller's ctx for lifecycle: the Subscriber's own ctx
// (cancelled by Stop) governs the fetch loop and retry goroutines, matching the
// pulsar transport's shutdown-channel model.
func (s *Subscriber) Subscribe(_ context.Context, name string) (<-chan ember.AckableEventEnvelope, error) {
	r, err := s.registry.Get(s.ctx, name)
	if err != nil {
		return nil, err
	}

	out := make(chan ember.AckableEventEnvelope)

	s.wg.Add(1)
	go s.run(r, out)

	return out, nil
}

// run is the per-reader fetch loop. It pulls one message at a time, registers
// it, and delivers it. FetchMessage returns an error when s.ctx is cancelled
// (Stop) or the reader is closed, which ends the loop.
func (s *Subscriber) run(r reader, out chan<- ember.AckableEventEnvelope) {
	defer s.wg.Done()
	tracker := newOffsetTracker()

	for {
		m, err := r.FetchMessage(s.ctx)
		if err != nil {
			return
		}
		tracker.register(m)
		s.deliver(r, tracker, out, m)
	}
}

// deliver unmarshals the payload, stamps metadata for the current attempt, and
// forwards the envelope. A malformed payload is poison (it will fail to
// unmarshal on every redelivery), so it is dropped and committed past rather
// than left to stall the partition.
func (s *Subscriber) deliver(r reader, tracker *offsetTracker, out chan<- ember.AckableEventEnvelope, m kafka.Message) {
	var msg message
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		s.logger.Error(s.ctx, "Could not unmarshal the message; dropping", err,
			"topic", m.Topic, "partition", m.Partition, "offset", m.Offset)
		s.commit(r, tracker, m)
		return
	}

	metadata := msg.Metadata
	if metadata == nil {
		metadata = make(ember.Metadata)
	}
	metadata[MetadataKeyCorrelationID] = msg.CorrelationID
	metadata[MetadataKeyCurrentDelivery] = tracker.attempt(m)
	if limit, capped := r.MaxDeliveries(); capped {
		metadata[MetadataKeyMaxDeliveries] = limit
	}

	envelope := ember.AckableEventEnvelope{
		EventEnvelope: ember.EventEnvelope{
			ID:       msg.ID,
			EntityID: msg.EntityID,
			Event: &ember.MarshaledEvent{
				Type: msg.Type,
				Data: msg.Data,
			},
			Metadata:  metadata,
			Timestamp: msg.PublishedAt,
		},
		Ack:  func() { s.commit(r, tracker, m) },
		Nack: func() { s.nack(r, tracker, out, m) },
	}

	select {
	case out <- envelope:
	case <-s.ctx.Done():
	}
}

// nack either schedules an in-session redelivery after the reader's backoff, or
// — when the delivery cap is reached — drops the message and commits past it so
// a poison message cannot stall the partition.
func (s *Subscriber) nack(r reader, tracker *offsetTracker, out chan<- ember.AckableEventEnvelope, m kafka.Message) {
	if limit, capped := r.MaxDeliveries(); capped && tracker.attempt(m) >= limit {
		s.logger.Warn(s.ctx, "Delivery cap reached; dropping message",
			"topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "max_deliveries", limit)
		s.commit(r, tracker, m)
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-time.After(r.RetryBackoff()):
		case <-s.ctx.Done():
			return
		}
		tracker.retry(m)
		s.deliver(r, tracker, out, m)
	}()
}

// commit advances the cumulative watermark and commits when it moves. A commit
// against a just-revoked partition (after a rebalance) may error; log and move on.
func (s *Subscriber) commit(r reader, tracker *offsetTracker, m kafka.Message) {
	cm, ok := tracker.complete(m)
	if !ok {
		return
	}
	if err := r.CommitMessages(s.ctx, cm); err != nil {
		s.logger.Error(s.ctx, "Could not commit offset", err,
			"topic", cm.Topic, "partition", cm.Partition, "offset", cm.Offset)
	}
}

func (s *Subscriber) Stop() {
	s.cancel()
	s.wg.Wait()
	if err := s.registry.Close(); err != nil {
		s.logger.Error(context.Background(), "Could not close consumer registry", err)
	}
}
