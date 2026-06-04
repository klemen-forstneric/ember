package dynamo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalData(t *testing.T) {
	in := []byte(`{"status":"open","total":100,"active":true,"missing":null,"addr":{"city":"NYC"},"tags":["a","b"]}`)
	got, err := marshalData(in)
	require.NoError(t, err)
	want := map[string]types.AttributeValue{
		"status":  &types.AttributeValueMemberS{Value: "open"},
		"total":   &types.AttributeValueMemberN{Value: "100"},
		"active":  &types.AttributeValueMemberBOOL{Value: true},
		"missing": &types.AttributeValueMemberNULL{Value: true},
		"addr": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"city": &types.AttributeValueMemberS{Value: "NYC"},
		}},
		"tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "a"},
			&types.AttributeValueMemberS{Value: "b"},
		}},
	}
	assert.Equal(t, want, got)
}

func TestMarshalDataEmpty(t *testing.T) {
	got, err := marshalData(nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// Round-trip must preserve an integer larger than 2^53, which a float64 decode
// would corrupt.
func TestDataRoundTripLargeInteger(t *testing.T) {
	in := []byte(`{"id":9007199254740993}`)
	av, err := marshalData(in)
	require.NoError(t, err)

	n, ok := av["id"].(*types.AttributeValueMemberN)
	require.True(t, ok, "expected an N attribute for id")
	assert.Equal(t, "9007199254740993", n.Value)

	out, err := unmarshalData(av)
	require.NoError(t, err)
	assert.JSONEq(t, `{"id":9007199254740993}`, string(out))
}
