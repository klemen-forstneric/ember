package mongo

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type EnsureOutboxSuite struct {
	suite.Suite
	col *mongo.Collection
}

func TestEnsureOutboxSuite(t *testing.T) {
	suite.Run(t, new(EnsureOutboxSuite))
}

func (s *EnsureOutboxSuite) SetupTest() {
	s.col = connectTestMongo(s.T())
}

func (s *EnsureOutboxSuite) TestCreatesIndexes() {
	ctx := context.Background()
	s.Require().NoError(EnsureOutbox(ctx, s.col))

	cur, err := s.col.Indexes().List(ctx)
	s.Require().NoError(err)
	var specs []bson.M
	s.Require().NoError(cur.All(ctx, &specs))

	var hasPartial, hasTTL bool
	for _, spec := range specs {
		if _, ok := spec["partialFilterExpression"]; ok {
			hasPartial = true
			s.Equal(bson.D{{Key: "seq", Value: int32(1)}}, spec["key"])
			s.Equal(bson.D{{Key: "published", Value: false}}, spec["partialFilterExpression"])
		}
		if _, ok := spec["expireAfterSeconds"]; ok {
			hasTTL = true
			s.Equal(bson.D{{Key: "expires_at", Value: int32(1)}}, spec["key"])
			s.Equal(int32(0), spec["expireAfterSeconds"])
		}
	}
	s.True(hasPartial, "expected a partial pending index")
	s.True(hasTTL, "expected a TTL index")
}

func (s *EnsureOutboxSuite) TestIdempotent() {
	ctx := context.Background()
	s.Require().NoError(EnsureOutbox(ctx, s.col))
	s.Require().NoError(EnsureOutbox(ctx, s.col), "second call must be a no-op, not an error")
}

type EnsureEntitiesSuite struct {
	suite.Suite
	col *mongo.Collection
}

func TestEnsureEntitiesSuite(t *testing.T) {
	suite.Run(t, new(EnsureEntitiesSuite))
}

func (s *EnsureEntitiesSuite) SetupTest() {
	s.col = connectTestMongo(s.T())
}

func legacyDoc(id, typ string, version uint64, n string) bson.D {
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "type", Value: typ},
		{Key: "version", Value: version},
		{Key: "data", Value: bson.D{{Key: "n", Value: n}}},
	}
}

// Documents written before entity_id existed carry identity on _id; after the
// backfill they must read and write through the (type, entity_id) key.
func (s *EnsureEntitiesSuite) TestBackfillsLegacyDocuments() {
	ctx := context.Background()
	_, err := s.col.InsertMany(ctx, []any{
		legacyDoc("old1", "order", 3, "a"),
		legacyDoc("old2", "offer", 1, "b"),
	})
	s.Require().NoError(err)

	s.Require().NoError(EnsureEntities(ctx, s.col))
	s.Require().NoError(EnsureEntities(ctx, s.col), "second call must be a no-op, not an error")

	repo, err := NewEntityRepository(ctx, s.col)
	s.Require().NoError(err)

	got, err := repo.Get(ctx, "order", "old1")
	s.Require().NoError(err)
	s.Equal(uint64(3), got.Version.Value())
	s.JSONEq(`{"n":"a"}`, string(got.Data))

	// A migrated document updates in place rather than gaining an ObjectId twin.
	s.Require().NoError(repo.Save(ctx, marshaled("order", "old1", 3, `{"n":"c"}`)))
	n, err := s.col.CountDocuments(ctx, bson.D{{Key: "type", Value: "order"}})
	s.Require().NoError(err)
	s.Equal(int64(1), n)
}

// Copying a non-string _id would produce an entity_id no Get could ever match,
// so such a document is left alone rather than silently orphaned.
func (s *EnsureEntitiesSuite) TestBackfillSkipsNonStringIDs() {
	ctx := context.Background()
	_, err := s.col.InsertOne(ctx, bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "type", Value: "order"},
		{Key: "version", Value: uint64(1)},
		{Key: "data", Value: bson.D{{Key: "n", Value: "a"}}},
	})
	s.Require().NoError(err)

	s.Require().NoError(EnsureEntities(ctx, s.col))

	n, err := s.col.CountDocuments(ctx, bson.D{{Key: "entity_id", Value: bson.D{{Key: "$exists", Value: true}}}})
	s.Require().NoError(err)
	s.Zero(n, "a non-string _id must not be copied into entity_id")
}

// The backfill bulk-writes in fixed-size batches, so a collection larger than one
// batch must still come out fully migrated rather than truncated at the boundary.
func (s *EnsureEntitiesSuite) TestBackfillsEveryDocumentAcrossBatches() {
	ctx := context.Background()
	const n = backfillBatch + 7

	docs := make([]any, 0, n)
	for i := range n {
		docs = append(docs, legacyDoc(strconv.Itoa(i), "order", 1, "a"))
	}
	_, err := s.col.InsertMany(ctx, docs)
	s.Require().NoError(err)

	s.Require().NoError(EnsureEntities(ctx, s.col))

	missing, err := s.col.CountDocuments(ctx, bson.D{{Key: "entity_id", Value: bson.D{{Key: "$exists", Value: false}}}})
	s.Require().NoError(err)
	s.Zero(missing)

	// Each entity_id must be its own document's _id, not a neighbour's.
	wrong, err := s.col.CountDocuments(ctx, bson.D{
		{Key: "$expr", Value: bson.D{{Key: "$ne", Value: bson.A{"$_id", "$entity_id"}}}},
	})
	s.Require().NoError(err)
	s.Zero(wrong)
}

func (s *EnsureEntitiesSuite) TestCreatesUniqueCompoundIndex() {
	ctx := context.Background()
	s.Require().NoError(EnsureEntities(ctx, s.col))

	cur, err := s.col.Indexes().List(ctx)
	s.Require().NoError(err)
	var specs []bson.M
	s.Require().NoError(cur.All(ctx, &specs))

	var found bool
	for _, spec := range specs {
		if spec["unique"] == true {
			found = true
			s.Equal(bson.D{{Key: "type", Value: int32(1)}, {Key: "entity_id", Value: int32(1)}}, spec["key"])
		}
	}
	s.True(found, "expected a unique (type, entity_id) index")
}
