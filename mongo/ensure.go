package mongo

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// namespaceExists is the mongo error code returned when a collection already
// exists (CreateCollection on an existing namespace).
const namespaceExists = 48

// EnsureCollection creates the collection if it does not already exist. Run it
// at startup — never inside a transaction: mongo/DocumentDB forbid implicit
// collection creation inside a multi-document transaction, so the first
// transactional insert would otherwise fail with "cannot create namespace ...".
// Idempotent: a NamespaceExists error is treated as success.
func EnsureCollection(ctx context.Context, c *mongo.Collection) error {
	err := c.Database().CreateCollection(ctx, c.Name())
	if err == nil {
		return nil
	}
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) && cmdErr.Code == namespaceExists {
		return nil
	}
	return err
}

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

// EnsureEntities provisions the entity collection's key. Run it at startup,
// before serving — never inside a transaction (index creation is DDL). Without
// it, Save cannot detect a version conflict and Get finds nothing.
//
// Documents written before entity_id existed carry identity on a string _id; the
// backfill copies it across. Those values are unique per collection by definition,
// so the index can never fail to build on them. Idempotent: the backfill only
// touches documents missing entity_id, and re-creating an identical index is a no-op.
func EnsureEntities(ctx context.Context, c *mongo.Collection) error {
	_, err := c.UpdateMany(
		ctx,
		bson.D{
			{Key: "entity_id", Value: bson.D{{Key: "$exists", Value: false}}},
			// A non-string _id would yield an entity_id no Get could ever match.
			{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}}},
		},
		// An aggregation pipeline, so entity_id takes _id's value rather than the literal.
		mongo.Pipeline{{{Key: "$set", Value: bson.D{{Key: "entity_id", Value: "$_id"}}}}},
	)
	if err != nil {
		return err
	}

	_, err = c.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "type", Value: 1}, {Key: "entity_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}
