package middleware

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/klemen-forstneric/ember"
)

// PanicRecover nacks an event whose consumer panicked instead of crashing the process.
func PanicRecover(l ember.LoggerCtx) ember.ConsumeMiddleware {
	return func(next ember.ConsumeFunc) ember.ConsumeFunc {
		return func(ctx context.Context, envelope ember.AckableEventEnvelope) {
			defer func() {
				r := recover()
				if r == nil {
					return
				}

				eventType := ""
				if envelope.Event != nil {
					eventType = envelope.Event.Type
				}
				l.Error(ctx, "Consumer panicked", fmt.Errorf("recovered: %v", r),
					"event_id", envelope.ID, "type", eventType, "entity_id", envelope.EntityID,
					"stack", string(debug.Stack()))
				envelope.Nack()
			}()

			next(ctx, envelope)
		}
	}
}
