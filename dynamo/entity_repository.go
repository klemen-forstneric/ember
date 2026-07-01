package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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

// Save writes the entity with a single conditional PutItem. The write succeeds
// when the item does not yet exist, or when its stored version equals the
// entity's initial (expected) version; otherwise it is a version conflict.
func (r *EntityRepository) Save(ctx context.Context, m *ember.MarshaledEntity) error {
	data, err := marshalData(m.Data)
	if err != nil {
		return err
	}

	item := map[string]types.AttributeValue{
		"type":    &types.AttributeValueMemberS{Value: m.Type},
		"id":      &types.AttributeValueMemberS{Value: m.ID},
		"version": &types.AttributeValueMemberN{Value: strconv.FormatUint(m.Version.Value(), 10)},
		"data":    &types.AttributeValueMemberM{Value: data},
	}

	cond := expression.AttributeNotExists(expression.Name("type")).
		Or(expression.Name("version").Equal(expression.Value(m.Version.Initial())))
	expr, err := expression.NewBuilder().WithCondition(cond).Build()
	if err != nil {
		return err
	}

	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(r.table),
		Item:                      item,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	var conflict *types.ConditionalCheckFailedException
	if errors.As(err, &conflict) {
		return ember.ErrVersionConflict
	}
	return err
}

func (r *EntityRepository) Get(ctx context.Context, typ, id string) (*ember.MarshaledEntity, error) {
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.table),
		Key: map[string]types.AttributeValue{
			"type": &types.AttributeValueMemberS{Value: typ},
			"id":   &types.AttributeValueMemberS{Value: id},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, ember.ErrEntityNotFound
	}
	return itemToEntity(out.Item)
}

func (r *EntityRepository) List(ctx context.Context, typ string, f ember.Filter, s ember.Sort) ([]*ember.MarshaledEntity, error) {
	if s.Path != "" {
		return nil, ember.ErrUnsupportedSort
	}
	filter, hasFilter, err := buildFilter(f)
	if err != nil {
		return nil, err
	}

	builder := expression.NewBuilder().
		WithKeyCondition(expression.Key("type").Equal(expression.Value(typ)))
	if hasFilter {
		builder = builder.WithFilter(filter)
	}
	expr, err := builder.Build()
	if err != nil {
		return nil, err
	}

	paginator := dynamodb.NewQueryPaginator(r.client, &dynamodb.QueryInput{
		TableName:                 aws.String(r.table),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(), // nil when there is no filter
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	var out []*ember.MarshaledEntity
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			e, err := itemToEntity(item)
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// itemToEntity decodes a stored item into a MarshaledEntity.
func itemToEntity(item map[string]types.AttributeValue) (*ember.MarshaledEntity, error) {
	id, ok := item["id"].(*types.AttributeValueMemberS)
	if !ok {
		return nil, fmt.Errorf("dynamo: item missing string id")
	}
	typ, ok := item["type"].(*types.AttributeValueMemberS)
	if !ok {
		return nil, fmt.Errorf("dynamo: item missing string type")
	}
	verAttr, ok := item["version"].(*types.AttributeValueMemberN)
	if !ok {
		return nil, fmt.Errorf("dynamo: item missing numeric version")
	}
	ver, err := strconv.ParseUint(verAttr.Value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("dynamo: invalid version %q: %w", verAttr.Value, err)
	}

	dataMap := map[string]types.AttributeValue{}
	if d, ok := item["data"].(*types.AttributeValueMemberM); ok {
		dataMap = d.Value
	}
	data, err := unmarshalData(dataMap)
	if err != nil {
		return nil, err
	}

	return &ember.MarshaledEntity{
		ID:      id.Value,
		Type:    typ.Value,
		Version: ember.NewVersion(ver),
		Data:    data,
	}, nil
}
