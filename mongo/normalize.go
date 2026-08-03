package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// NormalizeDocumentIDs replaces a string _id with a generated ObjectId on every
// document whose entity_id equals its _id — exactly the set EnsureEntities
// backfilled, so run that first. Anything else is left alone, which is what keeps
// it off the outbox, whose ids live only in _id.
//
// Operator-invoked with the service stopped; deliberately not called by
// NewEntityRepository or EnsureEntities, which run on every boot and must stay
// cheap. The returned count is a lower bound — it under-reports a retried commit
// and excludes the in-flight document on error.
func NormalizeDocumentIDs(ctx context.Context, client *mongo.Client, c *mongo.Collection) (int, error) {
	selector := bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}}}}
	projection := bson.D{{Key: "_id", Value: 1}, {Key: "entity_id", Value: 1}}

	// Collect the ids before deleting anything: mutating a collection while
	// iterating its own cursor can skip or revisit documents.
	cur, err := c.Find(ctx, selector, options.Find().SetProjection(projection))
	if err != nil {
		return 0, err
	}
	var rows []bson.Raw
	if err := cur.All(ctx, &rows); err != nil {
		return 0, err
	}

	stale := make([]string, 0, len(rows))
	for _, row := range rows {
		id, ok := convertible(row)
		if !ok {
			continue
		}
		stale = append(stale, id)
	}

	tx := NewTransactor(client)
	var n int
	for _, id := range stale {
		converted, err := normalizeOne(ctx, tx, c, id)
		if err != nil {
			return n, err
		}
		if converted {
			n++
		}
	}
	return n, nil
}

// convertible reports whether _id is duplicated into entity_id, so rewriting it
// cannot lose the document's identity. Compared as raw values so that a non-string
// entity_id fails the test rather than failing to decode.
func convertible(row bson.Raw) (string, bool) {
	if !row.Lookup("_id").Equal(row.Lookup("entity_id")) {
		return "", false
	}
	return row.Lookup("_id").StringValueOK()
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
