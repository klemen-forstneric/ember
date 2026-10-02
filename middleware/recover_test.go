package middleware_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/middleware"
)

func envelope(acks, nacks *int) ember.AckableEventEnvelope {
	return ember.AckableEventEnvelope{
		EventEnvelope: ember.EventEnvelope{ID: "e-1", EntityID: "pay-1", Event: &ember.MarshaledEvent{Type: "PaymentCreated"}},
		Ack:           func() { *acks++ },
		Nack:          func() { *nacks++ },
	}
}

func TestRecover_NacksAPanickingConsumer(t *testing.T) {
	logger := &mockLogger{}
	logger.On("Error", mock.Anything, "Consumer panicked", mock.MatchedBy(func(err error) bool { return err != nil }), mock.Anything).Return().Once()

	consume := middleware.Recover(logger)(func(context.Context, ember.AckableEventEnvelope) { panic("boom") })

	var acks, nacks int
	assert.NotPanics(t, func() { consume(context.Background(), envelope(&acks, &nacks)) })
	assert.Equal(t, 1, nacks)
	assert.Zero(t, acks)
	logger.AssertExpectations(t)
}

func TestRecover_LeavesAHealthyConsumerAlone(t *testing.T) {
	logger := &mockLogger{}
	called := false
	consume := middleware.Recover(logger)(func(_ context.Context, e ember.AckableEventEnvelope) {
		called = true
		e.Ack()
	})

	var acks, nacks int
	consume(context.Background(), envelope(&acks, &nacks))
	assert.True(t, called)
	assert.Equal(t, 1, acks)
	assert.Zero(t, nacks)
	logger.AssertNotCalled(t, "Error", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
