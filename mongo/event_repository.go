package mongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/klemen-forstneric/ember"
)

// EventRepository stores events in an outbox collection. Save is called by
// ember.Publisher (inside a transaction); ListUnpublished/MarkPublished are
// driven by the relay (ember.Relay).
type EventRepository struct {
	collection *mongo.Collection
}

func NewEventRepository(c *mongo.Collection) *EventRepository {
	return &EventRepository{collection: c}
}

// Data is a nested document, not bytes, so the payload is readable and
// queryable in the shell. MarshaledEvent.Data must be a JSON object.
type entry struct {
	ID          string         `bson:"_id"`
	EntityID    string         `bson:"entity_id"`
	Type        string         `bson:"type"`
	Data        bson.Raw       `bson:"data"`
	Metadata    ember.Metadata `bson:"metadata"`
	Version     uint64         `bson:"version"`
	Idx         int            `bson:"idx"`
	CreatedAt   time.Time      `bson:"created_at"`
	Published   bool           `bson:"published"`
	PublishedAt *time.Time     `bson:"published_at,omitempty"`
	ExpiresAt   *time.Time     `bson:"expires_at,omitempty"`
}

func (r *EventRepository) Save(ctx context.Context, envelopes []ember.EventEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	ds := make([]entry, 0, len(envelopes))
	for _, e := range envelopes {
		var data bson.Raw
		if err := bson.UnmarshalExtJSON(e.Event.Data, false, &data); err != nil {
			return err
		}

		ds = append(ds, entry{
			ID:        e.ID,
			EntityID:  e.EntityID,
			Type:      e.Event.Type,
			Data:      data,
			Metadata:  e.Metadata,
			Version:   e.Version,
			Idx:       e.Index,
			CreatedAt: e.Timestamp,
			Published: false,
		})
	}
	_, err := r.collection.InsertMany(ctx, ds)
	return err
}

func (r *EventRepository) ListUnpublished(ctx context.Context, limit int) ([]ember.EventEnvelope, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "entity_id", Value: 1}, {Key: "version", Value: 1}, {Key: "idx", Value: 1}})
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}
	filter := bson.D{{Key: "published", Value: false}}

	cur, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var out []ember.EventEnvelope
	for cur.Next(ctx) {
		var d entry
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}

		data, err := bson.MarshalExtJSON(d.Data, false, false)
		if err != nil {
			return nil, err
		}

		out = append(out, ember.EventEnvelope{
			ID:       d.ID,
			EntityID: d.EntityID,
			Version:  d.Version,
			Index:    d.Idx,
			Event: &ember.MarshaledEvent{
				Type: d.Type,
				Data: data,
			},
			Metadata:  d.Metadata,
			Timestamp: d.CreatedAt,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *EventRepository) MarkPublished(ctx context.Context, ids []string, expiresAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "published", Value: true},
		{Key: "published_at", Value: now},
		{Key: "expires_at", Value: expiresAt},
	}}}
	_, err := r.collection.UpdateMany(ctx, filter, update)
	return err
}
