package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func envelope(eventType, entityID string) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:        "evt-1",
		EntityID:  entityID,
		Event:     &ember.MarshaledEvent{Type: eventType, Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{MetadataKeyCorrelationID: "corr-1"},
		Timestamp: time.Unix(0, 0).UTC(),
	}
}

type PublisherSuite struct {
	suite.Suite
	w *mockWriter
}

func TestPublisherSuite(t *testing.T) {
	suite.Run(t, new(PublisherSuite))
}

func (s *PublisherSuite) SetupTest() {
	s.w = &mockWriter{}
}

func (s *PublisherSuite) TestRoutesByEventType() {
	s.w.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)
	p := NewPublisher(s.w, map[string]string{"order.created": "orders"})

	err := p.Publish(context.Background(), []ember.EventEnvelope{envelope("order.created", "e1")})
	s.Require().NoError(err)

	written := s.w.written()
	s.Require().Len(written, 1)
	s.Equal("orders", written[0].Topic)
	s.Equal("e1", string(written[0].Key))
}

func (s *PublisherSuite) TestMultipleTopicsInOneBatch() {
	s.w.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)
	p := NewPublisher(s.w, map[string]string{
		"order.created":   "orders",
		"payment.settled": "payments",
	})

	err := p.Publish(context.Background(), []ember.EventEnvelope{
		envelope("order.created", "e1"),
		envelope("payment.settled", "e2"),
	})
	s.Require().NoError(err)

	// A multi-topic publish must be a single batched WriteMessages call.
	s.w.AssertNumberOfCalls(s.T(), "WriteMessages", 1)
	written := s.w.written()
	s.Require().Len(written, 2)
	topics := map[string]bool{written[0].Topic: true, written[1].Topic: true}
	s.True(topics["orders"])
	s.True(topics["payments"])
}

func (s *PublisherSuite) TestUnmappedTypeErrors() {
	p := NewPublisher(s.w, map[string]string{})

	err := p.Publish(context.Background(), []ember.EventEnvelope{envelope("payment.refunded", "e1")})
	s.Error(err)
	s.w.AssertNotCalled(s.T(), "WriteMessages")
}

func (s *PublisherSuite) TestMissingCorrelationIDErrors() {
	p := NewPublisher(s.w, map[string]string{"order.created": "orders"})

	e := envelope("order.created", "e1")
	e.Metadata = ember.Metadata{} // no correlation id

	err := p.Publish(context.Background(), []ember.EventEnvelope{e})
	s.Error(err)
	s.w.AssertNotCalled(s.T(), "WriteMessages")
}

func (s *PublisherSuite) TestPropagatesWriteError() {
	s.w.On("WriteMessages", mock.Anything, mock.Anything).Return(errors.New("boom"))
	p := NewPublisher(s.w, map[string]string{"order.created": "orders"})

	err := p.Publish(context.Background(), []ember.EventEnvelope{envelope("order.created", "e1")})
	s.Error(err)
}

func (s *PublisherSuite) TestEmptyIsNoop() {
	p := NewPublisher(s.w, map[string]string{})

	err := p.Publish(context.Background(), []ember.EventEnvelope{})
	s.Require().NoError(err)
	s.w.AssertNotCalled(s.T(), "WriteMessages")
}

func (s *PublisherSuite) TestCloseClosesWriter() {
	s.w.On("Close").Return(nil)
	p := NewPublisher(s.w, map[string]string{})

	s.Require().NoError(p.Close())
	s.w.AssertCalled(s.T(), "Close")
}
