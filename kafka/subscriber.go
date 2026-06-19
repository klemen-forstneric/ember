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

// Subscriber
type Subscriber struct {
	registry consumerRegistry
	logger   ember.LoggerCtx

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewSubscriber(r consumerRegistry, l ember.LoggerCtx) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())

	return &Subscriber{
		registry: r,
		logger:   l,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Subscribe ignores the caller's ctx for lifecycle: the Subscriber's own ctx
// (cancelled by Stop) governs the fetch and retry loops, matching the pulsar
// transport's shutdown-channel model. Each call starts an independent session.
func (s *Subscriber) Subscribe(_ context.Context, name string) (<-chan ember.AckableEventEnvelope, error) {
	r, err := s.registry.Get(s.ctx, name)
	if err != nil {
		return nil, err
	}

	sess := &session{
		sub:     s,
		reader:  r,
		tracker: newOffsetTracker(),
		out:     make(chan ember.AckableEventEnvelope),
		signal:  make(chan struct{}, 1),
	}

	// Both goroutines are registered here, before any Stop can call wg.Wait, so
	// no wg.Add ever races wg.Wait (nack never touches the WaitGroup).
	s.wg.Add(2)
	go sess.fetchLoop()
	go sess.retryLoop()

	return sess.out, nil
}

func (s *Subscriber) Stop() {
	s.cancel()
	s.wg.Wait()
	if err := s.registry.Close(); err != nil {
		s.logger.Error(context.Background(), "Could not close consumer registry", err)
	}
}

// retryMessage is a message awaiting in-session redelivery once readyAt passes.
type retryMessage struct {
	m       kafka.Message
	readyAt time.Time
}

// session is the per-subscription engine for one reader. A fetch loop and a
// single retry loop fan into one out channel. Nacked messages are appended to
// an in-memory queue and re-delivered by the retry loop after the reader's
// backoff — one retry loop per reader, rather than a goroutine + timer per
// nacked message.
type session struct {
	sub     *Subscriber
	reader  reader
	tracker *offsetTracker
	out     chan ember.AckableEventEnvelope

	mu     sync.Mutex
	queue  []retryMessage
	signal chan struct{} // buffered(1) wakeup for the retry loop
}

// fetchLoop pulls one message at a time and delivers it. FetchMessage returns
// an error when the Subscriber's ctx is cancelled (Stop) or the reader closes,
// which ends the loop.
func (s *session) fetchLoop() {
	defer s.sub.wg.Done()
	for {
		m, err := s.reader.FetchMessage(s.sub.ctx)
		if err != nil {
			return
		}
		s.tracker.register(m)
		s.deliver(m)
	}
}

// retryLoop drains the retry queue: it waits for the head message's backoff to
// elapse, then re-delivers it. Backoff is constant per reader, so the queue is
// FIFO-ordered by readyAt and the head is always the soonest-due message.
func (s *session) retryLoop() {
	defer s.sub.wg.Done()
	for {
		select {
		case <-s.sub.ctx.Done():
			return
		default:
		}

		s.mu.Lock()
		if len(s.queue) == 0 {
			s.mu.Unlock()
			select {
			case <-s.signal:
				continue
			case <-s.sub.ctx.Done():
				return
			}
		}
		item := s.queue[0]
		s.mu.Unlock()

		if wait := time.Until(item.readyAt); wait > 0 {
			select {
			case <-time.After(wait):
			case <-s.sub.ctx.Done():
				return
			}
		}

		// Only this goroutine pops, so the head is still item; nack only appends
		// to the tail.
		s.mu.Lock()
		s.queue = s.queue[1:]
		if len(s.queue) == 0 {
			s.queue = nil // release the backing array when fully drained
		}
		s.mu.Unlock()

		s.tracker.retry(item.m)
		s.deliver(item.m)
	}
}

// deliver unmarshals the payload, stamps metadata for the current attempt, and
// forwards the envelope. A malformed payload is poison (it will fail to
// unmarshal on every redelivery), so it is dropped and committed past rather
// than left to stall the partition.
func (s *session) deliver(m kafka.Message) {
	var msg message
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		s.sub.logger.Error(s.sub.ctx, "Could not unmarshal the message; dropping", err,
			"topic", m.Topic, "partition", m.Partition, "offset", m.Offset)
		s.commit(m)
		return
	}

	metadata := msg.Metadata
	if metadata == nil {
		metadata = make(ember.Metadata)
	}
	metadata[MetadataKeyCorrelationID] = msg.CorrelationID
	metadata[ember.MetadataKeyCurrentDelivery] = s.tracker.attempt(m)
	if limit, capped := s.reader.MaxDeliveries(); capped {
		metadata[ember.MetadataKeyMaxDeliveries] = limit
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
		Ack:  func() { s.commit(m) },
		Nack: func() { s.retry(m) },
	}

	select {
	case s.out <- envelope:
	case <-s.sub.ctx.Done():
	}
}

// retry enqueues the message for in-session redelivery after the reader's
// backoff. The delivery cap is the retry budget: once it is exhausted the
// message is dropped and committed past so a poison message cannot stall the
// partition. Enqueuing is non-blocking: it must never block the downstream
// worker, because the retry loop re-delivers through the same out channel and
// would deadlock waiting for a worker that is itself blocked here.
func (s *session) retry(m kafka.Message) {
	if limit, capped := s.reader.MaxDeliveries(); capped && s.tracker.attempt(m) >= limit {
		s.sub.logger.Warn(s.sub.ctx, "Delivery cap reached; dropping message",
			"topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "max_deliveries", limit)
		s.commit(m)
		return
	}

	s.mu.Lock()
	s.queue = append(s.queue, retryMessage{m: m, readyAt: time.Now().Add(s.reader.RetryBackoff())})
	s.mu.Unlock()

	select {
	case s.signal <- struct{}{}:
	default: // a wakeup is already pending; the retry loop drains the whole queue
	}
}

// commit marks the offset done — whether the handler acked it, the delivery cap
// was reached, or the payload was poison — and advances the cumulative commit
// watermark, committing when it moves. A commit against a just-revoked partition
// (after a rebalance) may error; log and move on.
func (s *session) commit(m kafka.Message) {
	cm, ok := s.tracker.complete(m)
	if !ok {
		return
	}
	if err := s.reader.CommitMessages(s.sub.ctx, cm); err != nil {
		s.sub.logger.Error(s.sub.ctx, "Could not commit offset", err,
			"topic", cm.Topic, "partition", cm.Partition, "offset", cm.Offset)
	}
}
