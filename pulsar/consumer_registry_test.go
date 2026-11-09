package pulsar

import (
	"context"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// closableConsumer stands in for a *pulsar.Consumer the registry creates; only Close is called.
type closableConsumer struct {
	pulsar.Consumer
	closed int
}

func (c *closableConsumer) Close() { c.closed++ }

type ConsumerRegistrySuite struct {
	suite.Suite
	client *mockPulsarClient
}

func TestConsumerRegistrySuite(t *testing.T) { suite.Run(t, new(ConsumerRegistrySuite)) }

func (s *ConsumerRegistrySuite) SetupTest() {
	s.client = &mockPulsarClient{}
}

func (s *ConsumerRegistrySuite) TestRegisteredSubscriptionSubscribesWithItsOptions() {
	c := &closableConsumer{}
	opts := pulsar.ConsumerOptions{Topic: "ws.c1", SubscriptionName: "pod-1", Type: pulsar.Exclusive}
	s.client.On("Subscribe", opts).Return(c, nil)
	r := NewConsumerRegistry(s.client, nil)

	r.Register("ws.c1", opts)
	consumers, err := r.Get(context.Background(), "ws.c1")

	s.Require().NoError(err)
	s.Len(consumers, 1)
}

func (s *ConsumerRegistrySuite) TestReleaseClosesTheConsumersAndForgetsTheSubscription() {
	released, kept := &closableConsumer{}, &closableConsumer{}
	s.client.On("Subscribe", mock.MatchedBy(func(o pulsar.ConsumerOptions) bool { return o.Topic == "ws.c1" })).Return(released, nil)
	s.client.On("Subscribe", mock.MatchedBy(func(o pulsar.ConsumerOptions) bool { return o.Topic == "ws.c2" })).Return(kept, nil)
	r := NewConsumerRegistry(s.client, nil)
	r.Register("c1", pulsar.ConsumerOptions{Topic: "ws.c1"})
	r.Register("c2", pulsar.ConsumerOptions{Topic: "ws.c2"})
	ctx := context.Background()
	_, err := r.Get(ctx, "c1")
	s.Require().NoError(err)
	_, err = r.Get(ctx, "c2")
	s.Require().NoError(err)

	r.Release("c1")

	s.Equal(1, released.closed)
	s.Zero(kept.closed)
	_, err = r.Get(ctx, "c1")
	s.ErrorContains(err, "no consumer options", "a released subscription must be registered again")

	s.Require().NoError(r.Close())
	s.Equal(1, released.closed, "not closed twice")
	s.Equal(1, kept.closed)
}
