package pulsar

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
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

func twoEnvelopes(eventType string) []ember.EventEnvelope {
	e := envelope(eventType, "e1")
	e = append(e, ember.EventEnvelope{
		ID:        "evt-2",
		EntityID:  "e2",
		Event:     &ember.MarshaledEvent{Type: eventType, Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{MetadataKeyCorrelationID: "corr-2"},
		Timestamp: time.Unix(0, 0).UTC(),
	})
	return e
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

	n, err := p.Publish(context.Background(), envelope("order.created", "e1"))
	s.Require().NoError(err)
	s.Equal(1, n)

	sent := prod.sent()
	s.Require().Len(sent, 1)
	s.Equal("e1", sent[0].Key)
}

func (s *PublisherSuite) TestUnmappedTypeErrors() {
	s.reg.On("Get", mock.Anything, "payment.refunded").Return(nil, errors.New("unmapped event type"))
	p := NewPublisher(s.reg)

	n, err := p.Publish(context.Background(), envelope("payment.refunded", "e1"))
	s.Error(err)
	s.Equal(0, n)
}

func (s *PublisherSuite) TestMissingCorrelationIDErrors() {
	p := NewPublisher(s.reg)

	e := envelope("order.created", "e1")
	e[0].Metadata = ember.Metadata{} // no correlation id

	n, err := p.Publish(context.Background(), e)
	s.Error(err)
	s.Equal(0, n)
	// Validation happens before any producer is resolved.
	s.reg.AssertNotCalled(s.T(), "Get")
}

func (s *PublisherSuite) TestAggregatesSendErrors() {
	prod := &mockProducer{}
	prod.On("SendAsync", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("boom"))
	s.reg.On("Get", mock.Anything, "order.created").Return(prod, nil)
	p := NewPublisher(s.reg)

	n, err := p.Publish(context.Background(), envelope("order.created", "e1"))
	s.Error(err)
	s.Equal(0, n)
}

// TestPartialFailureReturnsLeadingRun pins the index-attributed error slice: a
// failure on the first send with the second succeeding must still report 0
// published, not the total success count, so the caller never marks past a gap.
func (s *PublisherSuite) TestPartialFailureReturnsLeadingRun() {
	prod := &mockProducer{}
	prod.On("SendAsync", mock.Anything, mock.MatchedBy(func(m *pulsar.ProducerMessage) bool {
		return m.Key == "e1"
	}), mock.Anything).Return(errors.New("boom"))
	prod.On("SendAsync", mock.Anything, mock.MatchedBy(func(m *pulsar.ProducerMessage) bool {
		return m.Key == "e2"
	}), mock.Anything).Return(nil)
	s.reg.On("Get", mock.Anything, "order.created").Return(prod, nil)
	p := NewPublisher(s.reg)

	n, err := p.Publish(context.Background(), twoEnvelopes("order.created"))
	s.Error(err)
	s.Equal(0, n, "first envelope failed, so nothing counts as a delivered prefix even though the second succeeded")
}

func (s *PublisherSuite) TestEmptyIsNoop() {
	p := NewPublisher(s.reg)

	n, err := p.Publish(context.Background(), []ember.EventEnvelope{})
	s.Require().NoError(err)
	s.Equal(0, n)
	s.reg.AssertNotCalled(s.T(), "Get")
}

func (s *PublisherSuite) TestCloseClosesRegistry() {
	s.reg.On("Close").Return(nil)
	p := NewPublisher(s.reg)

	s.Require().NoError(p.Close())
	s.reg.AssertNumberOfCalls(s.T(), "Close", 1)
}
