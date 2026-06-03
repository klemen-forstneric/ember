package dynamo

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

	"github.com/klemen-forstneric/ember"
)

// buildFilter translates a Filter into a DynamoDB FilterExpression condition.
// A nil filter returns ok=false, signalling the caller to omit the filter
// (match all). A non-nil filter returns ok=true with the condition.
func buildFilter(f ember.Filter) (expression.ConditionBuilder, bool, error) {
	if f == nil {
		return expression.ConditionBuilder{}, false, nil
	}
	cb, err := node(f)
	if err != nil {
		return expression.ConditionBuilder{}, false, err
	}
	return cb, true, nil
}

func node(f ember.Filter) (expression.ConditionBuilder, error) {
	switch n := f.(type) {
	case ember.Comparison:
		return comparison(n)
	case ember.Membership:
		return membership(n)
	case ember.Existence:
		return existence(n)
	case ember.Conjunction:
		return composite(n.Filters, true)
	case ember.Disjunction:
		return composite(n.Filters, false)
	case ember.Negation:
		inner, err := node(n.Filter)
		if err != nil {
			return expression.ConditionBuilder{}, err
		}
		return expression.Not(inner), nil
	default:
		return expression.ConditionBuilder{}, fmt.Errorf("%w: unknown node %T", ember.ErrUnsupportedFilter, f)
	}
}

func comparison(c ember.Comparison) (expression.ConditionBuilder, error) {
	name, reserved, err := attrName(c.Path)
	if err != nil {
		return expression.ConditionBuilder{}, err
	}
	v, err := normalizeValue(c.Value)
	if err != nil {
		return expression.ConditionBuilder{}, err
	}
	val := expression.Value(v)
	switch c.Op {
	case ember.OpEq:
		return name.Equal(val), nil
	case ember.OpNe:
		// Two-valued complement of Eq: present, non-null, and != v. A missing or
		// NULL-typed attribute must not match. Reserved paths are always present
		// and non-null, so they need no guard.
		if reserved {
			return name.NotEqual(val), nil
		}
		return expression.And(
			name.AttributeExists(),
			expression.Not(name.AttributeType(expression.Null)),
			name.NotEqual(val),
		), nil
	case ember.OpGt:
		return name.GreaterThan(val), nil
	case ember.OpGte:
		return name.GreaterThanEqual(val), nil
	case ember.OpLt:
		return name.LessThan(val), nil
	case ember.OpLte:
		return name.LessThanEqual(val), nil
	default:
		return expression.ConditionBuilder{}, fmt.Errorf("%w: operator %d", ember.ErrUnsupportedFilter, c.Op)
	}
}

func membership(m ember.Membership) (expression.ConditionBuilder, error) {
	name, _, err := attrName(m.Path)
	if err != nil {
		return expression.ConditionBuilder{}, err
	}
	if len(m.Values) == 0 {
		return alwaysFalse(), nil // empty IN matches nothing
	}
	ops := make([]expression.OperandBuilder, 0, len(m.Values))
	for _, raw := range m.Values {
		v, err := normalizeValue(raw)
		if err != nil {
			return expression.ConditionBuilder{}, err
		}
		ops = append(ops, expression.Value(v))
	}
	// A missing or NULL-typed attribute matches no string/number operand, so IN
	// is naturally the two-valued positive predicate; no guard needed.
	return name.In(ops[0], ops[1:]...), nil
}

func existence(e ember.Existence) (expression.ConditionBuilder, error) {
	name, reserved, err := attrName(e.Path)
	if err != nil {
		return expression.ConditionBuilder{}, err
	}
	if reserved {
		// version is always present and non-null.
		if e.Exists {
			return name.AttributeExists(), nil
		}
		return name.AttributeNotExists(), nil
	}
	if e.Exists {
		// present and non-null
		return expression.And(name.AttributeExists(), expression.Not(name.AttributeType(expression.Null))), nil
	}
	// absent or null
	return expression.Or(name.AttributeNotExists(), name.AttributeType(expression.Null)), nil
}

func composite(fs []ember.Filter, and bool) (expression.ConditionBuilder, error) {
	if len(fs) == 0 {
		if and {
			return alwaysTrue(), nil // empty AND matches all
		}
		return alwaysFalse(), nil // empty OR matches none
	}
	parts := make([]expression.ConditionBuilder, 0, len(fs))
	for _, f := range fs {
		p, err := node(f)
		if err != nil {
			return expression.ConditionBuilder{}, err
		}
		parts = append(parts, p)
	}
	if len(parts) == 1 {
		return parts[0], nil // And/Or require >=2 operands; a single child is itself
	}
	if and {
		return expression.And(parts[0], parts[1], parts[2:]...), nil
	}
	return expression.Or(parts[0], parts[1], parts[2:]...), nil
}

// attrName maps a filter path to a DynamoDB attribute name and reports whether
// it is a reserved (always-present, non-null) attribute. version is a plain
// top-level attribute; any other path addresses a nested attribute under data.
// id and type are key attributes and are rejected: DynamoDB forbids key
// attributes in a FilterExpression.
//
// Path segments must not contain "." (interpreted as nested separators by the
// expression package).
func attrName(path string) (name expression.NameBuilder, reserved bool, err error) {
	switch path {
	case "id", "type":
		return expression.NameBuilder{}, false,
			fmt.Errorf("%w: cannot filter on key attribute %q", ember.ErrUnsupportedFilter, path)
	case "version":
		return expression.Name("version"), true, nil
	default:
		return expression.Name("data." + path), false, nil
	}
}

// alwaysTrue / alwaysFalse encode logical identities. version is always present
// (every item carries it) and is not a key attribute, so it is safe to reference
// in a FilterExpression.
func alwaysTrue() expression.ConditionBuilder  { return expression.Name("version").AttributeExists() }
func alwaysFalse() expression.ConditionBuilder { return expression.Name("version").AttributeNotExists() }

// normalizeValue validates a filter value and converts time.Time to RFC3339Nano
// text, matching how time is serialized into the stored JSON data.
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
