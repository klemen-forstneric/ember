package middleware

import (
	"context"

	"github.com/google/uuid"
	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/correlation"
)

const (
	MetadataKeyCorrelationID ember.MetadataKey = "correlation_id"
)

func CorrelationID(l ember.LoggerCtx) ember.SubscriptionMiddleware {
	return func(next ember.HandleFunc) ember.HandleFunc {
		return func(ctx context.Context, e *ember.ReceivedEvent) error {
			correlationID, ok := e.Metadata[MetadataKeyCorrelationID].(string)
			if !ok {
				correlationID = uuid.NewString()
				l.Warn(ctx, "No correlation id found in metadata, creating a new one",
					"event_id", e.ID, "metadata", e.Metadata, "correlation_id", correlationID)
			}

			ctx = correlation.NewContext(ctx, correlationID)
			return next(ctx, e)
		}
	}
}
