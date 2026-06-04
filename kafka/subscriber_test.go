package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func kafkaMsgFor(t *testing.T, partition int, offset int64, eventType, entityID, correlationID string) kafka.Message {
	t.Helper()
	payload, err := json.Marshal(&message{
		ID:            "evt-1",
		CorrelationID: correlationID,
		EntityID:      entityID,
		Type:          eventType,
		Data:          []byte(`{"k":"v"}`),
		PublishedAt:   time.Unix(0, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "orders", Partition: partition, Offset: offset, Key: []byte(entityID), Value: payload}
}

type SubscriberSuite struct {
	suite.Suite
	reg *mockConsumerRegistry
	sub *Subscriber
}

func TestSubscriberSuite(t *testing.T) {
	suite.Run(t, new(SubscriberSuite))
}

func (s *SubscriberSuite) SetupTest() {
	s.reg = &mockConsumerRegistry{}
	s.sub = NewSubscriber(s.reg, ember.NopLogger)
}

// start wires a channel-backed reader with the given delivery cap and backoff
// into the registry, then subscribes. The reader's config getters and
// commit/close are optional (.Maybe) so each test asserts only the behavior it
// exercises — commits are verified through reader.committed().
func (s *SubscriberSuite) start(maxDel int, capped bool, backoff time.Duration) (*mockReader, <-chan ember.AckableEventEnvelope) {
	r := newMockReader()
	r.On("MaxDeliveries").Return(maxDel, capped).Maybe()
	r.On("RetryBackoff").Return(backoff).Maybe()
	r.On("CommitMessages", mock.Anything, mock.Anything).Return(nil).Maybe()
	r.On("Close").Return(nil).Maybe()
	s.reg.On("Get", mock.Anything, "projector").Return(r, nil)
	s.reg.On("Close").Return(nil).Maybe()

	out, err := s.sub.Subscribe(context.Background(), "projector")
	s.Require().NoError(err)
	return r, out
}

func (s *SubscriberSuite) TestForwardsStampsAndCommitsOnAck() {
	r, out := s.start(5, true, time.Millisecond)

	r.in <- kafkaMsgFor(s.T(), 0, 7, "order.created", "e1", "corr-1")

	select {
	case env := <-out:
		s.Equal("e1", env.EntityID)
		s.Equal(1, env.Metadata[MetadataKeyCurrentDelivery])
		s.Equal(5, env.Metadata[MetadataKeyMaxDeliveries])
		s.Equal("corr-1", env.Metadata[MetadataKeyCorrelationID])
		env.Ack()
	case <-time.After(time.Second):
		s.FailNow("timed out waiting for an envelope")
	}

	s.sub.Stop()

	commits := r.committed()
	s.Require().Len(commits, 1)
	s.Equal(int64(7), commits[0].Offset)
}

func (s *SubscriberSuite) TestOmitsMaxDeliveriesWhenUncapped() {
	r, out := s.start(0, false, time.Millisecond)

	r.in <- kafkaMsgFor(s.T(), 0, 0, "order.created", "e1", "corr-1")

	select {
	case env := <-out:
		s.Equal(1, env.Metadata[MetadataKeyCurrentDelivery])
		s.NotContains(env.Metadata, MetadataKeyMaxDeliveries)
		env.Ack()
	case <-time.After(time.Second):
		s.FailNow("timed out waiting for an envelope")
	}

	s.sub.Stop()
}

func (s *SubscriberSuite) TestCommitsContiguouslyUnderOutOfOrderAcks() {
	r, out := s.start(5, true, time.Millisecond)

	// Three messages on one partition, offsets 1,2,3.
	r.in <- kafkaMsgFor(s.T(), 0, 1, "order.created", "e1", "c")
	r.in <- kafkaMsgFor(s.T(), 0, 2, "order.created", "e2", "c")
	r.in <- kafkaMsgFor(s.T(), 0, 3, "order.created", "e3", "c")

	// Receive all three before acking, keyed by entity id.
	envs := map[string]ember.AckableEventEnvelope{}
	for i := 0; i < 3; i++ {
		select {
		case env := <-out:
			envs[env.EntityID] = env
		case <-time.After(time.Second):
			s.FailNow("timed out waiting for envelopes")
		}
	}

	// Ack out of order: e3 (offset 3) first commits nothing; then e1, then e2.
	envs["e3"].Ack()
	s.Empty(r.committed(), "no commit expected after acking offset 3 alone")

	envs["e1"].Ack()
	envs["e2"].Ack()

	s.sub.Stop()

	commits := r.committed()
	s.Require().Len(commits, 2)
	s.Equal(int64(1), commits[0].Offset)
	s.Equal(int64(3), commits[1].Offset)
}

func (s *SubscriberSuite) TestRetriesNackedMessageThenCommits() {
	r, out := s.start(3, true, time.Millisecond)

	r.in <- kafkaMsgFor(s.T(), 0, 4, "order.created", "e1", "c")

	// First delivery: attempt 1, nack it.
	select {
	case env := <-out:
		s.Equal(1, env.Metadata[MetadataKeyCurrentDelivery])
		env.Nack()
	case <-time.After(time.Second):
		s.FailNow("timed out on first delivery")
	}

	// Redelivery: attempt 2, ack it.
	select {
	case env := <-out:
		s.Equal(2, env.Metadata[MetadataKeyCurrentDelivery])
		env.Ack()
	case <-time.After(time.Second):
		s.FailNow("timed out on redelivery")
	}

	s.sub.Stop()

	commits := r.committed()
	s.Require().Len(commits, 1)
	s.Equal(int64(4), commits[0].Offset)
}

func (s *SubscriberSuite) TestDropsAndCommitsWhenCapReached() {
	r, out := s.start(2, true, time.Millisecond) // cap of 2 deliveries

	r.in <- kafkaMsgFor(s.T(), 0, 9, "order.created", "e1", "c")

	// Attempt 1 -> nack -> retried.
	(<-out).Nack()
	// Attempt 2 -> nack -> cap reached -> dropped + committed.
	select {
	case env := <-out:
		s.Equal(2, env.Metadata[MetadataKeyCurrentDelivery])
		env.Nack()
	case <-time.After(time.Second):
		s.FailNow("timed out on second delivery")
	}

	// No third delivery should arrive.
	select {
	case env := <-out:
		s.FailNowf("unexpected third delivery", "got entity %q", env.EntityID)
	case <-time.After(50 * time.Millisecond):
	}

	s.sub.Stop()

	commits := r.committed()
	s.Require().Len(commits, 1)
	s.Equal(int64(9), commits[0].Offset)
}

func (s *SubscriberSuite) TestDropsAndCommitsMalformedPayload() {
	r, out := s.start(5, true, time.Millisecond)

	r.in <- kafka.Message{Topic: "orders", Partition: 0, Offset: 11, Value: []byte("not json")}

	// Nothing is delivered to the handler.
	select {
	case env := <-out:
		s.FailNowf("unexpected delivery for malformed payload", "got entity %q", env.EntityID)
	case <-time.After(100 * time.Millisecond):
	}

	s.sub.Stop()

	commits := r.committed()
	s.Require().Len(commits, 1)
	s.Equal(int64(11), commits[0].Offset)
}

func (s *SubscriberSuite) TestUnknownSubscriptionErrors() {
	s.reg.On("Get", mock.Anything, "nope").Return(nil, errors.New("unknown subscription"))

	_, err := s.sub.Subscribe(context.Background(), "nope")
	s.Error(err)
}

func (s *SubscriberSuite) TestGetErrorPropagates() {
	s.reg.On("Get", mock.Anything, "projector").Return(nil, errors.New("boom"))

	_, err := s.sub.Subscribe(context.Background(), "projector")
	s.Error(err)
}

func (s *SubscriberSuite) TestStopClosesRegistry() {
	s.start(1, true, time.Millisecond)

	s.sub.Stop()

	s.reg.AssertNumberOfCalls(s.T(), "Close", 1)
}

func (s *SubscriberSuite) TestNackAfterStopIsSafeNoop() {
	r, out := s.start(3, true, time.Millisecond)

	r.in <- kafkaMsgFor(s.T(), 0, 1, "order.created", "e1", "c")

	var env ember.AckableEventEnvelope
	select {
	case env = <-out:
	case <-time.After(time.Second):
		s.FailNow("timed out waiting for an envelope")
	}

	s.sub.Stop()

	// A downstream handler may still nack after the transport has stopped (ember
	// stops the transport before the consumer). This must not panic and must not
	// schedule a redelivery: the retry loop has already exited, so the enqueued
	// message is simply never re-delivered.
	env.Nack()

	select {
	case <-out:
		s.FailNow("did not expect a redelivery after Stop")
	case <-time.After(20 * time.Millisecond):
	}
}

func (s *SubscriberSuite) TestRetriesMultipleNackedMessages() {
	// A backoff well above the time to read three channel messages guarantees the
	// three initial deliveries (current_delivery 1) are all received before any
	// redelivery (current_delivery 2) is re-emitted, making the assertions below
	// order-independent of the retry loop.
	r, out := s.start(5, true, 50*time.Millisecond)

	// Three messages on one partition. Each is delivered, nacked once, then
	// re-delivered by the single retry loop (no goroutine per message).
	r.in <- kafkaMsgFor(s.T(), 0, 1, "order.created", "e1", "c")
	r.in <- kafkaMsgFor(s.T(), 0, 2, "order.created", "e2", "c")
	r.in <- kafkaMsgFor(s.T(), 0, 3, "order.created", "e3", "c")

	for i := 0; i < 3; i++ {
		select {
		case env := <-out:
			s.Equal(1, env.Metadata[MetadataKeyCurrentDelivery])
			env.Nack()
		case <-time.After(time.Second):
			s.FailNow("timed out on first deliveries")
		}
	}

	// All three are re-delivered (attempt 2) via the retry queue; ack each.
	for i := 0; i < 3; i++ {
		select {
		case env := <-out:
			s.Equal(2, env.Metadata[MetadataKeyCurrentDelivery])
			env.Ack()
		case <-time.After(time.Second):
			s.FailNow("timed out on redeliveries")
		}
	}

	s.sub.Stop()

	commits := r.committed()
	s.Require().Len(commits, 3)
	s.Equal(int64(3), commits[2].Offset)
}
