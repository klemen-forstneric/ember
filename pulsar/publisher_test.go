package pulsar

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func envelope(eventType, entityID string) []ember.EventEnvelope {
	return []ember.EventEnvelope{{
		ID:        "evt-1",
		EntityID:  entityID,
		Event:     &ember.MarshaledEvent{Type: eventType, Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{MetadataKeyCorrelationID: "corr-1"},
		Timestamp: time.Unix(0, 0).UTC(),
	}}
}

type PublisherSuite struct {
	suite.Suite
	reg *mockProducerRegistry
}

func TestPublisherSuite(t *testing.T) {
	suite.Run(t, new(PublisherSuite))
}

func (s *PublisherSuite) SetupTest() {
	s.reg = &mockProducerRegistry{}
}

func (s *PublisherSuite) TestRoutesByEventType() {
	prod := &mockProducer{}
	prod.On("SendAsync", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.reg.On("Get", mock.Anything, "order.created").Return(prod, nil)
	p := NewPublisher(s.reg)

	err := p.Publish(context.Background(), envelope("order.created", "e1"))
	s.Require().NoError(err)

	sent := prod.sent()
	s.Require().Len(sent, 1)
	s.Equal("e1", sent[0].Key)
}

func (s *PublisherSuite) TestUnmappedTypeErrors() {
	s.reg.On("Get", mock.Anything, "payment.refunded").Return(nil, errors.New("unmapped event type"))
	p := NewPublisher(s.reg)

	err := p.Publish(context.Background(), envelope("payment.refunded", "e1"))
	s.Error(err)
}

func (s *PublisherSuite) TestMissingCorrelationIDErrors() {
	p := NewPublisher(s.reg)

	e := envelope("order.created", "e1")
	e[0].Metadata = ember.Metadata{} // no correlation id

	err := p.Publish(context.Background(), e)
	s.Error(err)
	// Validation happens before any producer is resolved.
	s.reg.AssertNotCalled(s.T(), "Get")
}

func (s *PublisherSuite) TestAggregatesSendErrors() {
	prod := &mockProducer{}
	prod.On("SendAsync", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("boom"))
	s.reg.On("Get", mock.Anything, "order.created").Return(prod, nil)
	p := NewPublisher(s.reg)

	err := p.Publish(context.Background(), envelope("order.created", "e1"))
	s.Error(err)
}

func (s *PublisherSuite) TestEmptyIsNoop() {
	p := NewPublisher(s.reg)

	err := p.Publish(context.Background(), []ember.EventEnvelope{})
	s.Require().NoError(err)
	s.reg.AssertNotCalled(s.T(), "Get")
}

func (s *PublisherSuite) TestCloseClosesRegistry() {
	s.reg.On("Close").Return(nil)
	p := NewPublisher(s.reg)

	s.Require().NoError(p.Close())
	s.reg.AssertNumberOfCalls(s.T(), "Close", 1)
}
