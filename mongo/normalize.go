package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// NormalizeDocumentIDs replaces every string _id with a generated ObjectId,
// preserving the rest of the document. Operator-invoked with the service stopped:
// it is deliberately not called by NewEntityRepository or EnsureEntities, which
// run on every boot of every service and must stay cheap. Returns the number of
// documents converted.
//
// _id is immutable, so each document is deleted and reinserted inside one
// transaction — a crash can neither lose an entity nor leave a duplicate, and
// re-running converts only what is left. Documents without entity_id are skipped;
// rewriting their _id would destroy their only identity, so run EnsureEntities
// first.
func NormalizeDocumentIDs(ctx context.Context, client *mongo.Client, c *mongo.Collection) (int, error) {
	selector := bson.D{
		{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}}},
		{Key: "entity_id", Value: bson.D{{Key: "$exists", Value: true}}},
	}

	// Collect the ids before deleting anything: mutating a collection while
	// iterating its own cursor can skip or revisit documents.
	cur, err := c.Find(ctx, selector, options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return 0, err
	}
	var stale []struct {
		ID string `bson:"_id"`
	}
	if err := cur.All(ctx, &stale); err != nil {
		return 0, err
	}

	tx := NewTransactor(client)
	var n int
	for _, doc := range stale {
		converted, err := normalizeOne(ctx, tx, c, doc.ID)
		if err != nil {
			return n, err
		}
		if converted {
			n++
		}
	}
	return n, nil
}

// normalizeOne reinserts one document without its _id. Delete must precede
// insert: the other order trips the unique (type, entity_id) index against the
// original, which is still present.
func normalizeOne(ctx context.Context, tx *Transactor, c *mongo.Collection, id string) (bool, error) {
	var converted bool
	err := tx.WithinTx(ctx, func(txCtx context.Context) error {
		converted = false // WithinTx may retry the callback

		var doc bson.D
		if err := c.FindOneAndDelete(txCtx, bson.D{{Key: "_id", Value: id}}).Decode(&doc); err == mongo.ErrNoDocuments {
			return nil
		} else if err != nil {
			return err
		}

		if _, err := c.InsertOne(txCtx, withoutID(doc)); err != nil {
			return err
		}
		converted = true
		return nil
	})
	return converted, err
}

func withoutID(doc bson.D) bson.D {
	out := make(bson.D, 0, len(doc))
	for _, e := range doc {
		if e.Key != "_id" {
			out = append(out, e)
		}
	}
	return out
}
