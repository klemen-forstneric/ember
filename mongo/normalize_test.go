package mongo

import (
	"context"
	"fmt"
	"testing"
	"time"

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

func (s *NormalizeDocumentIDsSuite) rawDocument(filter bson.D) bson.Raw {
	raw, err := s.col.FindOne(context.Background(), filter).Raw()
	s.Require().NoError(err)
	return raw
}

// fieldBytes maps every top-level field except _id to its BSON type and raw
// bytes. It walks the document itself rather than reusing withoutID, so a
// regression in withoutID cannot hide behind the assertion.
func (s *NormalizeDocumentIDsSuite) fieldBytes(raw bson.Raw) map[string][]byte {
	elems, err := raw.Elements()
	s.Require().NoError(err)

	out := make(map[string][]byte, len(elems))
	for _, e := range elems {
		if e.Key() == "_id" {
			continue
		}
		v := e.Value()
		out[e.Key()] = append([]byte{byte(v.Type)}, v.Value...)
	}
	return out
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

// An outbox entry also has a string _id and an entity_id, but they differ: the
// event's id lives only in _id, so converting one would destroy it and leave the
// relay unable to decode its own backlog.
func (s *NormalizeDocumentIDsSuite) TestLeavesOutboxEntriesAlone() {
	ctx := context.Background()
	s.Require().NoError(EnsureOutbox(ctx, s.col))

	events := NewEventRepository(s.col)
	base := time.Unix(1_700_000_000, 0).UTC()
	s.Require().NoError(events.Save(ctx, []ember.EventEnvelope{
		env("evt1", "ent1", base.Add(1*time.Millisecond)),
		env("evt2", "ent2", base.Add(2*time.Millisecond)),
	}))
	before := s.fieldBytes(s.rawDocument(bson.D{{Key: "_id", Value: "evt1"}}))

	s.Equal(0, s.normalize(), "an outbox entry must never be converted")

	s.Equal(int64(2), s.countIDsOfType("string"), "every event _id must survive")

	// The relay must still be able to read its own backlog.
	got, err := events.ListUnpublished(ctx, 10)
	s.Require().NoError(err)
	s.Equal([]string{"evt1", "evt2"}, ids(got))

	s.Equal(before, s.fieldBytes(s.rawDocument(bson.D{{Key: "_id", Value: "evt1"}})))
}

// An earlier revision's unguarded backfill produced ObjectId entity_id values.
// Converting such a document would delete the last string identifying it.
func (s *NormalizeDocumentIDsSuite) TestSkipsNonStringEntityID() {
	ctx := context.Background()
	_, err := s.col.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "old1"},
		{Key: "entity_id", Value: bson.NewObjectID()},
		{Key: "type", Value: "order"},
		{Key: "version", Value: uint64(1)},
		{Key: "data", Value: bson.D{{Key: "n", Value: "a"}}},
	})
	s.Require().NoError(err)

	s.Equal(0, s.normalize())
	s.Equal(int64(1), s.countIDsOfType("string"))
}

// Every field is carried across, not just the four the document struct knows.
func (s *NormalizeDocumentIDsSuite) TestPreservesUnrecognisedFields() {
	ctx := context.Background()
	_, err := s.col.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "old1"},
		{Key: "entity_id", Value: "old1"},
		{Key: "type", Value: "order"},
		{Key: "version", Value: uint64(1)},
		{Key: "data", Value: bson.D{{Key: "n", Value: "a"}}},
		{Key: "extra", Value: bson.D{{Key: "deep", Value: bson.A{int32(1), "two"}}}},
		{Key: "blob", Value: bson.Binary{Subtype: 0x00, Data: []byte{0xde, 0xad}}},
	})
	s.Require().NoError(err)
	before := s.fieldBytes(s.rawDocument(bson.D{{Key: "_id", Value: "old1"}}))

	s.Equal(1, s.normalize())

	after := s.fieldBytes(s.rawDocument(bson.D{{Key: "entity_id", Value: "old1"}}))
	s.Equal(before, after, "every non-_id field must survive byte for byte")
}

// The delete and the reinsert must commit together. A validator that rejects a
// non-string _id makes the reinsert fail deterministically; without the
// transaction the delete would stand and the document would be gone.
func (s *NormalizeDocumentIDsSuite) TestFailedReinsertRollsBackTheDelete() {
	ctx := context.Background()
	s.seedLegacy(legacyDoc("old1", "order", 1, "a"))
	before := s.fieldBytes(s.rawDocument(bson.D{{Key: "_id", Value: "old1"}}))

	s.Require().NoError(s.col.Database().RunCommand(ctx, bson.D{
		{Key: "collMod", Value: s.col.Name()},
		{Key: "validator", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "$type", Value: "string"}}},
		}},
	}).Err())

	n, err := NormalizeDocumentIDs(ctx, s.col.Database().Client(), s.col)
	s.Require().Error(err, "the reinsert must fail")
	s.Zero(n)

	s.Equal(int64(1), s.count(), "the delete must roll back with it")
	s.Equal(int64(1), s.countIDsOfType("string"))
	s.Equal(before, s.fieldBytes(s.rawDocument(bson.D{{Key: "_id", Value: "old1"}})))
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
