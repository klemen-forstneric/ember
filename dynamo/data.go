package dynamo

import (
	"bytes"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// marshalData decodes the entity's JSON data into a DynamoDB document map.
// Numbers are decoded with UseNumber so large integers are not rounded through
// float64; they are stored in the native numeric type (N).
func marshalData(b []byte) (map[string]types.AttributeValue, error) {
	if len(b) == 0 {
		return map[string]types.AttributeValue{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	out := make(map[string]types.AttributeValue, len(raw))
	for k, v := range raw {
		out[k] = toAttributeValue(v)
	}
	return out, nil
}

func toAttributeValue(v any) types.AttributeValue {
	switch x := v.(type) {
	case nil:
		return &types.AttributeValueMemberNULL{Value: true}
	case bool:
		return &types.AttributeValueMemberBOOL{Value: x}
	case string:
		return &types.AttributeValueMemberS{Value: x}
	case json.Number:
		return &types.AttributeValueMemberN{Value: x.String()}
	case map[string]any:
		m := make(map[string]types.AttributeValue, len(x))
		for k, e := range x {
			m[k] = toAttributeValue(e)
		}
		return &types.AttributeValueMemberM{Value: m}
	case []any:
		l := make([]types.AttributeValue, len(x))
		for i, e := range x {
			l[i] = toAttributeValue(e)
		}
		return &types.AttributeValueMemberL{Value: l}
	default:
		// With UseNumber, json decoding yields only the cases above; treat any
		// unexpected value as null rather than panicking.
		return &types.AttributeValueMemberNULL{Value: true}
	}
}

// unmarshalData converts a DynamoDB document map back into JSON bytes. Numeric
// attributes become json.Number so json.Marshal re-emits the exact literal.
func unmarshalData(m map[string]types.AttributeValue) ([]byte, error) {
	raw := make(map[string]any, len(m))
	for k, v := range m {
		raw[k] = fromAttributeValue(v)
	}
	return json.Marshal(raw)
}

func fromAttributeValue(v types.AttributeValue) any {
	switch x := v.(type) {
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberBOOL:
		return x.Value
	case *types.AttributeValueMemberS:
		return x.Value
	case *types.AttributeValueMemberN:
		return json.Number(x.Value)
	case *types.AttributeValueMemberM:
		m := make(map[string]any, len(x.Value))
		for k, e := range x.Value {
			m[k] = fromAttributeValue(e)
		}
		return m
	case *types.AttributeValueMemberL:
		l := make([]any, len(x.Value))
		for i, e := range x.Value {
			l[i] = fromAttributeValue(e)
		}
		return l
	default:
		// Only the types produced by marshalData are ever read back.
		return nil
	}
}
