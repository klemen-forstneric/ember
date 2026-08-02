package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/klemen-forstneric/ember"
)

// EntityRepository stores entities keyed by (type, entity_id). _id is a
// meaningless surrogate; EnsureEntities builds the index that enforces the key.
type EntityRepository struct {
	collection *mongo.Collection
}

// NewEntityRepository provisions the key before returning, so a repository
// cannot exist without the index its optimistic lock rides on.
func NewEntityRepository(ctx context.Context, c *mongo.Collection) (*EntityRepository, error) {
	if err := EnsureEntities(ctx, c); err != nil {
		return nil, err
	}
	return &EntityRepository{collection: c}, nil
}

type document struct {
	EntityID string   `bson:"entity_id"`
	Type     string   `bson:"type"`
	Version  uint64   `bson:"version"`
	Data     bson.Raw `bson:"data"`
}

func (d document) entity() (*ember.MarshaledEntity, error) {
	data, err := bson.MarshalExtJSON(d.Data, false, false)
	if err != nil {
		return nil, err
	}
	return &ember.MarshaledEntity{
		ID:      d.EntityID,
		Type:    d.Type,
		Version: ember.NewVersion(d.Version),
		Data:    data,
	}, nil
}

func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	var body bson.Raw
	if err := bson.UnmarshalExtJSON(m.Data, false, &body); err != nil {
		return err
	}

	// _id is absent, so an upsert that inserts gets a freshly generated ObjectId.
	filter := bson.D{
		{Key: "type", Value: m.Type},
		{Key: "entity_id", Value: m.ID},
		{Key: "version", Value: m.Version.Initial()},
	}

	replacement := document{
		EntityID: m.ID,
		Type:     m.Type,
		Version:  m.Version.Value(),
		Data:     body,
	}

	_, err := r.collection.ReplaceOne(
		ctx,
		filter,
		replacement,
		options.Replace().SetUpsert(true),
	)

	// Missing on version falls through to an insert, which the unique
	// (type, entity_id) index rejects: the row exists at another version.
	if mongo.IsDuplicateKeyError(err) {
		return ember.ErrVersionConflict
	}

	return err
}

func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	filter := bson.D{
		{Key: "type", Value: typ},
		{Key: "entity_id", Value: id},
	}

	var d document
	if err := r.collection.FindOne(ctx, filter).Decode(&d); err == mongo.ErrNoDocuments {
		return nil, ember.ErrEntityNotFound
	} else if err != nil {
		return nil, err
	}

	return d.entity()
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
		var d document
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}

		m, err := d.entity()
		if err != nil {
			return nil, err
		}

		out = append(out, m)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
