package mongo

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/klemen-forstneric/ember"
)

type NormalizeDocumentIDsSuite struct {
	suite.Suite
	col *mongo.Collection
}

func TestNormalizeDocumentIDsSuite(t *testing.T) {
	suite.Run(t, new(NormalizeDocumentIDsSuite))
}

func (s *NormalizeDocumentIDsSuite) SetupTest() {
	s.col = connectTestMongo(s.T())
}

func (s *NormalizeDocumentIDsSuite) normalize() int {
	n, err := NormalizeDocumentIDs(context.Background(), s.col.Database().Client(), s.col)
	s.Require().NoError(err)
	return n
}

func (s *NormalizeDocumentIDsSuite) countIDsOfType(bsonType string) int64 {
	n, err := s.col.CountDocuments(context.Background(),
		bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: bsonType}}}})
	s.Require().NoError(err)
	return n
}

func (s *NormalizeDocumentIDsSuite) count() int64 {
	n, err := s.col.CountDocuments(context.Background(), bson.D{})
	s.Require().NoError(err)
	return n
}

// seedLegacy inserts pre-migration documents (string _id, no entity_id) and runs
// the backfill, leaving the collection in the state an operator would find it.
func (s *NormalizeDocumentIDsSuite) seedLegacy(docs ...bson.D) {
	ctx := context.Background()
	as := make([]any, len(docs))
	for i, d := range docs {
		as[i] = d
	}
	_, err := s.col.InsertMany(ctx, as)
	s.Require().NoError(err)
	s.Require().NoError(EnsureEntities(ctx, s.col))
}

func (s *NormalizeDocumentIDsSuite) TestConvertsEveryStringIDAndPreservesEntities() {
	ctx := context.Background()
	s.seedLegacy(legacyDoc("old1", "order", 3, "a"), legacyDoc("old2", "offer", 1, "b"))
	before := s.count()

	s.Equal(2, s.normalize())

	s.Equal(before, s.count(), "nothing may be lost")
	s.Zero(s.countIDsOfType("string"))
	s.Equal(int64(2), s.countIDsOfType("objectId"))

	repo, err := NewEntityRepository(ctx, s.col)
	s.Require().NoError(err)

	order, err := repo.Get(ctx, "order", "old1")
	s.Require().NoError(err)
	s.Equal("old1", order.ID)
	s.Equal("order", order.Type)
	s.Equal(uint64(3), order.Version.Value())
	s.JSONEq(`{"n":"a"}`, string(order.Data))

	offer, err := repo.Get(ctx, "offer", "old2")
	s.Require().NoError(err)
	s.Equal(uint64(1), offer.Version.Value())
	s.JSONEq(`{"n":"b"}`, string(offer.Data))
}

func (s *NormalizeDocumentIDsSuite) TestIdempotent() {
	s.seedLegacy(legacyDoc("old1", "order", 1, "a"))

	s.Equal(1, s.normalize())
	s.Equal(0, s.normalize(), "a normalised collection must be a no-op")
}

func (s *NormalizeDocumentIDsSuite) TestLeavesObjectIDDocumentsAlone() {
	ctx := context.Background()
	s.seedLegacy(legacyDoc("old1", "order", 1, "a"))

	repo, err := NewEntityRepository(ctx, s.col)
	s.Require().NoError(err)
	s.Require().NoError(repo.Save(ctx, marshaled("order", "fresh", 0, `{"n":"c"}`)))

	fresh, err := s.col.FindOne(ctx, bson.D{{Key: "entity_id", Value: "fresh"}}).Raw()
	s.Require().NoError(err)
	idBefore := fresh.Lookup("_id")

	s.Equal(1, s.normalize(), "only the string _id is converted")

	fresh, err = s.col.FindOne(ctx, bson.D{{Key: "entity_id", Value: "fresh"}}).Raw()
	s.Require().NoError(err)
	s.Equal(idBefore, fresh.Lookup("_id"), "an ObjectId _id must be untouched")
}

// Rewriting the _id of a document that has no entity_id would destroy its only
// identity, so the backfill has to have run first.
func (s *NormalizeDocumentIDsSuite) TestSkipsDocumentsWithoutEntityID() {
	ctx := context.Background()
	_, err := s.col.InsertOne(ctx, legacyDoc("old1", "order", 1, "a"))
	s.Require().NoError(err)

	s.Equal(0, s.normalize())
	s.Equal(int64(1), s.countIDsOfType("string"))
}

// Ids are collected before any delete, so a collection large enough to span
// several cursor batches still converts every document exactly once.
func (s *NormalizeDocumentIDsSuite) TestConvertsEveryDocumentAcrossBatches() {
	const total = 200

	docs := make([]bson.D, total)
	for i := range docs {
		docs[i] = legacyDoc(fmt.Sprintf("old%d", i), "order", 1, fmt.Sprintf("n%d", i))
	}
	s.seedLegacy(docs...)

	s.Equal(total, s.normalize())
	s.Equal(int64(total), s.count())
	s.Zero(s.countIDsOfType("string"))

	ctx := context.Background()
	repo, err := NewEntityRepository(ctx, s.col)
	s.Require().NoError(err)
	for i := range total {
		got, err := repo.Get(ctx, "order", fmt.Sprintf("old%d", i))
		s.Require().NoError(err)
		s.JSONEq(fmt.Sprintf(`{"n":"n%d"}`, i), string(got.Data))
	}
}

// The key is enforced by the (type, entity_id) index, not by _id, so converting
// _id must not weaken the optimistic lock.
func (s *NormalizeDocumentIDsSuite) TestUniqueIndexStillEnforcesAfterwards() {
	ctx := context.Background()
	s.seedLegacy(legacyDoc("old1", "order", 1, "a"))
	s.Require().Equal(1, s.normalize())

	repo, err := NewEntityRepository(ctx, s.col)
	s.Require().NoError(err)

	s.Require().NoError(repo.Save(ctx, marshaled("order", "old1", 1, `{"n":"b"}`)))
	s.ErrorIs(repo.Save(ctx, marshaled("order", "old1", 1, `{"n":"c"}`)), ember.ErrVersionConflict)

	got, err := repo.Get(ctx, "order", "old1")
	s.Require().NoError(err)
	s.JSONEq(`{"n":"b"}`, string(got.Data), "the losing write must not land")
}
