package dynamo

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestMarshalData(t *testing.T) {
	in := []byte(`{"status":"open","total":100,"active":true,"missing":null,"addr":{"city":"NYC"},"tags":["a","b"]}`)
	got, err := marshalData(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestMarshalDataEmpty(t *testing.T) {
	got, err := marshalData(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %#v, want empty map", got)
	}
}

// Round-trip must preserve an integer larger than 2^53, which a float64 decode
// would corrupt.
func TestDataRoundTripLargeInteger(t *testing.T) {
	in := []byte(`{"id":9007199254740993}`)
	av, err := marshalData(in)
	if err != nil {
		t.Fatalf("marshalData: %v", err)
	}
	n, ok := av["id"].(*types.AttributeValueMemberN)
	if !ok || n.Value != "9007199254740993" {
		t.Fatalf("got %#v, want N 9007199254740993", av["id"])
	}
	out, err := unmarshalData(av)
	if err != nil {
		t.Fatalf("unmarshalData: %v", err)
	}
	if string(out) != `{"id":9007199254740993}` {
		t.Errorf("round-trip got %s, want {\"id\":9007199254740993}", out)
	}
}
