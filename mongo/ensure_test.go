package mongo

import (
	"context"
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
