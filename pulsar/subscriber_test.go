package pulsar

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// stubMessage is a minimal pulsar.Message: only the methods the Subscriber
// touches return meaningful values.
type stubMessage struct {
	pulsar.Message
	payload     []byte
	redelivered uint32
}

func (m stubMessage) Payload() []byte         { return m.payload }
func (m stubMessage) RedeliveryCount() uint32 { return m.redelivered }

func msgFor(t *testing.T, eventType, entityID, correlationID string, redelivered uint32) pulsar.ConsumerMessage {
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
	return pulsar.ConsumerMessage{Message: stubMessage{payload: payload, redelivered: redelivered}}
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

// newConsumer builds a channel-backed consumer with the given delivery cap. Its
// Ack/Nack/MaxDeliveries are optional (.Maybe) so each test asserts only the
// behavior it exercises, via callCount.
func (s *SubscriberSuite) newConsumer(maxDel int, capped bool) *mockConsumer {
	c := newMockConsumer()
	c.On("MaxDeliveries").Return(maxDel, capped).Maybe()
	c.On("Ack", mock.Anything).Return(nil).Maybe()
	c.On("Nack", mock.Anything).Return().Maybe()
	c.On("Close").Return().Maybe()
	return c
}

// start registers the consumers under "projector" and subscribes.
func (s *SubscriberSuite) start(consumers ...consumer) <-chan ember.AckableEventEnvelope {
	s.reg.On("Get", mock.Anything, "projector").Return(consumers, nil)
	s.reg.On("Close").Return(nil).Maybe()

	out, err := s.sub.Subscribe(context.Background(), "projector")
	s.Require().NoError(err)
	return out
}

func (s *SubscriberSuite) TestForwardsAndStampsMetadata() {
	c := s.newConsumer(5, true)
	out := s.start(c)

	c.in <- msgFor(s.T(), "order.created", "e1", "corr-1", 2)

	select {
	case env := <-out:
		s.Equal("e1", env.EntityID)
		s.Equal(3, env.Metadata[MetadataKeyCurrentDelivery])
		s.Equal(5, env.Metadata[MetadataKeyMaxDeliveries])
		s.Equal("corr-1", env.Metadata[MetadataKeyCorrelationID])
		env.Ack()
	case <-time.After(time.Second):
		s.FailNow("timed out waiting for an envelope")
	}

	s.sub.Stop()
	s.Equal(1, c.callCount("Ack"))
}

func (s *SubscriberSuite) TestOmitsMaxDeliveriesWhenUncapped() {
	c := s.newConsumer(0, false)
	out := s.start(c)

	c.in <- msgFor(s.T(), "order.created", "e1", "corr-1", 0)

	select {
	case env := <-out:
		s.Equal(1, env.Metadata[MetadataKeyCurrentDelivery])
		s.NotContains(env.Metadata, MetadataKeyMaxDeliveries)
	case <-time.After(time.Second):
		s.FailNow("timed out waiting for an envelope")
	}

	s.sub.Stop()
}

func (s *SubscriberSuite) TestFansInMultipleConsumers() {
	a, b := s.newConsumer(1, true), s.newConsumer(1, true)
	out := s.start(a, b)

	a.in <- msgFor(s.T(), "order.created", "e1", "c", 0)
	b.in <- msgFor(s.T(), "order.created", "e2", "c", 0)

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case env := <-out:
			seen[env.EntityID] = true
		case <-time.After(time.Second):
			s.FailNow("timed out")
		}
	}
	s.True(seen["e1"])
	s.True(seen["e2"])

	s.sub.Stop()
}

func (s *SubscriberSuite) TestNackInvokesConsumer() {
	c := s.newConsumer(1, true)
	out := s.start(c)

	c.in <- msgFor(s.T(), "order.created", "e1", "corr-1", 0)

	select {
	case env := <-out:
		env.Nack()
	case <-time.After(time.Second):
		s.FailNow("timed out waiting for an envelope")
	}

	s.sub.Stop()
	s.Equal(1, c.callCount("Nack"))
	s.Equal(0, c.callCount("Ack"))
}

func (s *SubscriberSuite) TestUnknownSubscriptionErrors() {
	s.reg.On("Get", mock.Anything, "nope").Return(nil, errors.New("unknown subscription"))

	_, err := s.sub.Subscribe(context.Background(), "nope")
	s.Error(err)
}

func (s *SubscriberSuite) TestStopClosesRegistry() {
	s.start(s.newConsumer(1, true))

	s.sub.Stop()

	s.reg.AssertNumberOfCalls(s.T(), "Close", 1)
}
