package middleware_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/middleware"
)

func lockedIdempotent(t *testing.T, handle ember.HandleFunc) (ember.HandleFunc, *mockLock) {
	t.Helper()
	lock := &mockLock{}
	locker := &mockLocker{}
	locker.On("TryLock", mock.Anything, "evt_e-1").Return(lock, nil)
	return middleware.Idempotent("evt", locker, ember.NopLogger)(handle), lock
}

func TestIdempotent_ReleasesTheLockWhenTheHandlerFails(t *testing.T) {
	h, lock := lockedIdempotent(t, func(context.Context, *ember.ReceivedEvent) error { return errors.New("boom") })
	lock.On("Release", mock.Anything).Return(nil).Once()

	require.Error(t, h(context.Background(), &ember.ReceivedEvent{ID: "e-1"}))
	lock.AssertExpectations(t)
}

func TestIdempotent_KeepsTheLockWhenTheHandlerSucceeds(t *testing.T) {
	h, lock := lockedIdempotent(t, func(context.Context, *ember.ReceivedEvent) error { return nil })

	require.NoError(t, h(context.Background(), &ember.ReceivedEvent{ID: "e-1"}))
	lock.AssertNotCalled(t, "Release", mock.Anything)
}

// A panic must not leave the lock held, or the redelivery is skipped as already
// handled and the event is lost.
func TestIdempotent_ReleasesTheLockWhenTheHandlerPanics(t *testing.T) {
	h, lock := lockedIdempotent(t, func(context.Context, *ember.ReceivedEvent) error { panic("boom") })
	lock.On("Release", mock.Anything).Return(nil).Once()

	assert.PanicsWithValue(t, "boom", func() { _ = h(context.Background(), &ember.ReceivedEvent{ID: "e-1"}) })
	lock.AssertExpectations(t)
}
