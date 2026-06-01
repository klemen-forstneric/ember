package mongo

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/klemen-forstneric/ember"
)

// buildFilter translates a Filter into a MongoDB query document. A nil filter
// yields an empty document (match all).
func buildFilter(f ember.Filter) (bson.D, error) {
	if f == nil {
		return bson.D{}, nil
	}
	return node(f)
}

func node(f ember.Filter) (bson.D, error) {
	switch n := f.(type) {
	case ember.Comparison:
		op, err := mongoOp(n.Op)
		if err != nil {
			return nil, err
		}
		v, err := normalizeValue(n.Value)
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: field(n.Path), Value: bson.D{{Key: op, Value: v}}}}, nil
	case ember.Membership:
		vals := make(bson.A, 0, len(n.Values))
		for _, raw := range n.Values {
			v, err := normalizeValue(raw)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return bson.D{{Key: field(n.Path), Value: bson.D{{Key: "$in", Value: vals}}}}, nil
	case ember.Existence:
		return bson.D{{Key: field(n.Path), Value: bson.D{{Key: "$exists", Value: n.Exists}}}}, nil
	case ember.Conjunction:
		if len(n.Filters) == 0 {
			return bson.D{}, nil // empty AND matches all
		}
		return composite("$and", n.Filters)
	case ember.Disjunction:
		if len(n.Filters) == 0 {
			// empty OR matches nothing; {} matches all, so $nor of {} matches none
			return bson.D{{Key: "$nor", Value: bson.A{bson.D{}}}}, nil
		}
		return composite("$or", n.Filters)
	case ember.Negation:
		inner, err := node(n.Filter)
		if err != nil {
			return nil, err
		}
		// $nor with a single element is the standard top-level negation in
		// MongoDB; $not is only valid as a field-level operator.
		return bson.D{{Key: "$nor", Value: bson.A{inner}}}, nil
	default:
		return nil, fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, f)
	}
}

func composite(op string, fs []ember.Filter) (bson.D, error) {
	arr := make(bson.A, 0, len(fs))
	for _, f := range fs {
		d, err := node(f)
		if err != nil {
			return nil, err
		}
		arr = append(arr, d)
	}
	return bson.D{{Key: op, Value: arr}}, nil
}

// field maps a filter path to a Mongo field path. Reserved metadata paths map to
// top-level fields; everything else lives under the data document.
func field(path string) string {
	switch path {
	case "id":
		return "_id"
	case "type", "version":
		return path
	default:
		return "data." + path
	}
}

func mongoOp(op ember.Operator) (string, error) {
	switch op {
	case ember.OpEq:
		return "$eq", nil
	case ember.OpNe:
		return "$ne", nil
	case ember.OpGt:
		return "$gt", nil
	case ember.OpGte:
		return "$gte", nil
	case ember.OpLt:
		return "$lt", nil
	case ember.OpLte:
		return "$lte", nil
	default:
		return "", fmt.Errorf("%w: operator %d", ember.ErrUnsupportedFilter, op)
	}
}

// normalizeValue validates a filter value. time.Time is converted to an
// RFC3339Nano string because the entity serializer encodes time.Time as an
// RFC3339 string in JSON, which bson.UnmarshalExtJSON then stores as a BSON
// string. All other primitives pass through unchanged to match their native
// BSON type in the data subdocument.
func normalizeValue(v any) (any, error) {
	switch x := v.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return x, nil
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), nil
	default:
		return nil, fmt.Errorf("%w: value type %T", ember.ErrUnsupportedFilter, v)
	}
}
