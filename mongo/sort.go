package mongo

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/klemen-forstneric/ember"
)

const (
	sortAscending  = 1
	sortDescending = -1
)

func sortDoc(s ember.Sort, paged bool) bson.D {
	dir := sortAscending
	if s.Direction == ember.Descending {
		dir = sortDescending
	}

	switch {
	case s.Path == "" && !paged:
		return nil
	case s.Path == "":
		return bson.D{{Key: "entity_id", Value: sortAscending}}
	case !paged:
		return bson.D{{Key: field(s.Path), Value: dir}}
	default:
		return bson.D{{Key: field(s.Path), Value: dir}, {Key: "entity_id", Value: dir}}
	}
}

func seekPredicate(s ember.Sort, c ember.Cursor) (bson.D, error) {
	if s.Path == "" {
		return bson.D{{Key: "entity_id", Value: bson.D{{Key: "$gt", Value: c.ID}}}}, nil
	}

	op := "$gt"
	if s.Direction == ember.Descending {
		op = "$lt"
	}

	tie := bson.D{{Key: "entity_id", Value: bson.D{{Key: op, Value: c.ID}}}}

	if c.Value == nil {
		return nil, fmt.Errorf("%w: sorted cursor requires a value", ember.ErrInvalidCursor)
	}

	v, err := normalizeValue(c.Value)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ember.ErrInvalidCursor, err)
	}

	f := field(s.Path)

	return bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: f, Value: bson.D{{Key: op, Value: v}}}},
		bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: f, Value: v}},
			tie,
		}}},
	}}}, nil
}
