package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// EnsureOutbox provisions the outbox collection's indexes. Run it at startup —
// never inside a transaction (index creation is DDL). Creating an index
// materializes the collection, so the first transactional insert finds an
// existing namespace. Idempotent: re-creating identical indexes is a no-op.
func EnsureOutbox(ctx context.Context, c *mongo.Collection) error {
	models := []mongo.IndexModel{
		{
			// Pending scan: only documents with published:false are indexed,
			// so the index shrinks to the backlog. Equality is required —
			// mongo partial filters do not allow $exists:false.
			Keys: bson.D{{Key: "seq", Value: 1}},
			Options: options.Index().
				SetPartialFilterExpression(bson.D{{Key: "published", Value: false}}),
		},
		{
			// TTL: mongo deletes a published doc once expires_at passes.
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0),
		},
	}
	_, err := c.Indexes().CreateMany(ctx, models)
	return err
}
