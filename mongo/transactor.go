package mongo

import (
	"context"

	"github.com/klemen-forstneric/ember"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Transactor runs work inside a mongo transaction. Reentrant: if the ctx already
// carries a session (an outer WithinTx), it joins that transaction instead of
// starting a nested one, which mongo forbids.
type Transactor struct {
	client *mongo.Client
}

func NewTransactor(client *mongo.Client) *Transactor {
	return &Transactor{client: client}
}

var _ ember.Transactor = (*Transactor)(nil)

func (t *Transactor) InTx(ctx context.Context) bool {
	return mongo.SessionFromContext(ctx) != nil
}

func (t *Transactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	if t.InTx(ctx) {
		return fn(ctx)
	}

	sess, err := t.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(txCtx context.Context) (any, error) {
		return nil, fn(txCtx)
	})
	return err
}
