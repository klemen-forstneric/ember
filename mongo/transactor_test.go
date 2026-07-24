package mongo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type TransactorSuite struct {
	suite.Suite
	client *mongo.Client
	tx     *Transactor
}

func TestTransactorSuite(t *testing.T) {
	suite.Run(t, new(TransactorSuite))
}

func (s *TransactorSuite) SetupTest() {
	// connectTestMongo (sort_test.go, same package) skips when mongo is
	// unavailable; reuse its client. StartSession needs no replica set.
	s.client = connectTestMongo(s.T()).Database().Client()
	s.tx = NewTransactor(s.client)
}

// When the ctx already carries a session, WithinTx joins it: it runs fn on the
// same session-bound ctx and does not start a second session/transaction.
func (s *TransactorSuite) TestWithinTxJoinsExistingSession() {
	sess, err := s.client.StartSession()
	s.Require().NoError(err)
	defer sess.EndSession(context.Background())

	sctx := mongo.NewSessionContext(context.Background(), sess)

	called := false
	err = s.tx.WithinTx(sctx, func(ctx context.Context) error {
		called = true
		s.Equal(sess.ID(), mongo.SessionFromContext(ctx).ID(), "fn must receive the same session")
		return nil
	})

	s.Require().NoError(err)
	s.True(called)
}
