package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/klemen-forstneric/ember"
)

// EntityRepository persists MarshaledEntity values in a single DynamoDB table
// keyed by partition attribute "type" and sort attribute "id".
type EntityRepository struct {
	client *dynamodb.Client
	table  string
}

// NewEntityRepository returns a repository backed by the given client and table.
// The caller owns table creation: partition key "type" (S), sort key "id" (S).
func NewEntityRepository(client *dynamodb.Client, table string) *EntityRepository {
	return &EntityRepository{client: client, table: table}
}

func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	return errors.New("not implemented")
}

func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	return nil, errors.New("not implemented")
}

func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter) ([]*ember.MarshaledEntity, error) {
	return nil, errors.New("not implemented")
}
