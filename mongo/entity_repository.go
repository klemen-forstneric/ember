package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/klemen-forstneric/ember"
)

// EntityRepository
type EntityRepository struct {
	collection *mongo.Collection
}

func NewEntityRepository(c *mongo.Collection) *EntityRepository {
	return &EntityRepository{collection: c}
}

func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	var body bson.D
	if err := bson.UnmarshalExtJSON(m.Data, false, &body); err != nil {
		return err
	}

	filter := bson.D{
		{Key: "_id", Value: m.ID},
		{Key: "version", Value: m.Version.Initial()},
	}

	replacement := bson.D{
		{Key: "type", Value: m.Type},
		{Key: "version", Value: m.Version.Value()},
		{Key: "data", Value: body},
	}

	_, err := r.collection.ReplaceOne(
		ctx,
		filter,
		replacement,
		options.Replace().SetUpsert(true),
	)

	if mongo.IsDuplicateKeyError(err) {
		return ember.ErrVersionConflict
	}

	return err
}

func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	filter := bson.D{
		{Key: "_id", Value: id},
		{Key: "type", Value: typ},
	}

	res := r.collection.FindOne(ctx, filter)

	var e struct {
		ID      string   `bson:"_id"`
		Type    string   `bson:"type"`
		Version uint64   `bson:"version"`
		Data    bson.Raw `bson:"data"`
	}

	if err := res.Decode(&e); err == mongo.ErrNoDocuments {
		return nil, ember.ErrEntityNotFound
	} else if err != nil {
		return nil, err
	}

	data, err := bson.MarshalExtJSON(e.Data, false, false)
	if err != nil {
		return nil, err
	}

	return &ember.MarshaledEntity{
		ID:      e.ID,
		Type:    e.Type,
		Version: ember.NewVersion(e.Version),
		Data:    data,
	}, nil
}

func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter, s ember.Sort) ([]*ember.MarshaledEntity, error) {
	predicate, err := buildFilter(f)
	if err != nil {
		return nil, err
	}

	filter := bson.D{{Key: "type", Value: typ}}
	if len(predicate) > 0 {
		filter = bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "type", Value: typ}},
			predicate,
		}}}
	}

	opts := options.Find()
	if s.Path != "" {
		dir := 1
		if s.Direction == ember.Descending {
			dir = -1
		}
		opts.SetSort(bson.D{{Key: field(s.Path), Value: dir}})
	}

	cur, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var out []*ember.MarshaledEntity
	for cur.Next(ctx) {
		var e struct {
			ID      string   `bson:"_id"`
			Type    string   `bson:"type"`
			Version uint64   `bson:"version"`
			Data    bson.Raw `bson:"data"`
		}
		if err := cur.Decode(&e); err != nil {
			return nil, err
		}

		data, err := bson.MarshalExtJSON(e.Data, false, false)
		if err != nil {
			return nil, err
		}

		out = append(out, &ember.MarshaledEntity{
			ID:      e.ID,
			Type:    e.Type,
			Version: ember.NewVersion(e.Version),
			Data:    data,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
