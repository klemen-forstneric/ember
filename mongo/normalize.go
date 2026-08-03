package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// normalizeChunk is how many documents move per transaction. At the ~760 byte
// average this repository stores that is well under a megabyte of transaction
// log against DocumentDB's 32MB ceiling, with room for documents an order of
// magnitude larger.
const normalizeChunk = 500

// NormalizeDocumentIDs replaces a string _id with a generated ObjectId on every
// document whose entity_id equals its _id — exactly the set EnsureEntities
// backfilled, so run that first. Anything else is left alone.
//
// Operator-invoked with the service stopped; deliberately not called by
// NewEntityRepository or EnsureEntities, which run on every boot and must stay
// cheap. Nothing may write to the collection while it runs — a concurrent write
// aborts the affected chunk rather than silently duplicating a document.
//
// The returned count is a lower bound: it under-reports a retried commit and
// excludes an in-flight chunk on error. Re-running finishes the job.
func NormalizeDocumentIDs(ctx context.Context, client *mongo.Client, c *mongo.Collection) (int, error) {
	stale, err := staleIDs(ctx, c)
	if err != nil {
		return 0, err
	}

	tx := NewTransactor(client)
	var n int
	for start := 0; start < len(stale); start += normalizeChunk {
		moved, err := normalizeBatch(ctx, tx, c, stale[start:min(start+normalizeChunk, len(stale))])
		n += moved
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// staleIDs lists the string _ids safe to rewrite. Projected rather than read
// whole so the id list stays small however large the documents are.
func staleIDs(ctx context.Context, c *mongo.Collection) ([]string, error) {
	selector := bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}}}}
	projection := bson.D{{Key: "_id", Value: 1}, {Key: "entity_id", Value: 1}}

	cur, err := c.Find(ctx, selector, options.Find().SetProjection(projection))
	if err != nil {
		return nil, err
	}
	var rows []bson.Raw
	if err := cur.All(ctx, &rows); err != nil {
		return nil, err
	}

	stale := make([]string, 0, len(rows))
	for _, row := range rows {
		id, ok := convertible(row)
		if !ok {
			continue
		}
		stale = append(stale, id)
	}
	return stale, nil
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

// normalizeBatch reinserts one chunk of documents without their _id, in a single
// transaction. The read is deliberately outside it: DocumentDB does not support
// cursors within a transaction, and with no concurrent writers the two steps see
// the same documents — which the delete count then verifies.
func normalizeBatch(ctx context.Context, tx *Transactor, c *mongo.Collection, ids []string) (int, error) {
	cur, err := c.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return 0, err
	}
	var docs []bson.D
	if err := cur.All(ctx, &docs); err != nil {
		return 0, err
	}

	found := make([]any, 0, len(docs))
	bodies := make([]any, 0, len(docs))
	for _, doc := range docs {
		id, ok := documentID(doc)
		if !ok {
			continue
		}
		found = append(found, id)
		bodies = append(bodies, withoutID(doc))
	}
	if len(found) == 0 {
		return 0, nil
	}

	var moved int
	err = tx.WithinTx(ctx, func(txCtx context.Context) error {
		moved = 0 // WithinTx may retry the callback

		// Delete before insert: the other order trips the unique (type, entity_id)
		// index against the originals, which are still present.
		res, err := c.DeleteMany(txCtx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: found}}}})
		if err != nil {
			return err
		}
		if int(res.DeletedCount) != len(found) {
			return fmt.Errorf("ember: read %d documents from %s but deleted %d — the collection was written to concurrently",
				len(found), c.Name(), res.DeletedCount)
		}

		if _, err := c.InsertMany(txCtx, bodies); err != nil {
			return err
		}
		moved = len(bodies)
		return nil
	})
	return moved, err
}

func documentID(doc bson.D) (string, bool) {
	for _, e := range doc {
		if e.Key == "_id" {
			id, ok := e.Value.(string)
			return id, ok
		}
	}
	return "", false
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
