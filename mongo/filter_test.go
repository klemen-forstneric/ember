package mongo

import (
	"errors"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/klemen-forstneric/ember"
)

func TestBuildFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter ember.Filter
		want   bson.D
	}{
		{"nil matches all", nil, bson.D{}},
		{"eq data path", ember.Eq("status", "open"), bson.D{{Key: "data.status", Value: bson.D{{Key: "$eq", Value: "open"}}}}},
		{"nested data path", ember.Eq("address.city", "NYC"), bson.D{{Key: "data.address.city", Value: bson.D{{Key: "$eq", Value: "NYC"}}}}},
		{"reserved id", ember.Eq("id", "x"), bson.D{{Key: "_id", Value: bson.D{{Key: "$eq", Value: "x"}}}}},
		{"reserved version", ember.Gt("version", 5), bson.D{{Key: "version", Value: bson.D{{Key: "$gt", Value: 5}}}}},
		{"gt", ember.Gt("total", 100), bson.D{{Key: "data.total", Value: bson.D{{Key: "$gt", Value: 100}}}}},
		{"in", ember.In("region", "EU", "UK"), bson.D{{Key: "data.region", Value: bson.D{{Key: "$in", Value: bson.A{"EU", "UK"}}}}}},
		{"exists", ember.Exists("status", true), bson.D{{Key: "data.status", Value: bson.D{{Key: "$exists", Value: true}}}}},
		{
			"and",
			ember.And(ember.Eq("a", "1"), ember.Eq("b", "2")),
			bson.D{{Key: "$and", Value: bson.A{
				bson.D{{Key: "data.a", Value: bson.D{{Key: "$eq", Value: "1"}}}},
				bson.D{{Key: "data.b", Value: bson.D{{Key: "$eq", Value: "2"}}}},
			}}},
		},
		{
			"or",
			ember.Or(ember.Eq("a", "1"), ember.Eq("b", "2")),
			bson.D{{Key: "$or", Value: bson.A{
				bson.D{{Key: "data.a", Value: bson.D{{Key: "$eq", Value: "1"}}}},
				bson.D{{Key: "data.b", Value: bson.D{{Key: "$eq", Value: "2"}}}},
			}}},
		},
		{
			"not",
			ember.Not(ember.Eq("status", "open")),
			bson.D{{Key: "$nor", Value: bson.A{
				bson.D{{Key: "data.status", Value: bson.D{{Key: "$eq", Value: "open"}}}},
			}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildFilter(tt.filter)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBuildFilterUnsupportedValue(t *testing.T) {
	_, err := buildFilter(ember.Eq("status", []string{"nope"}))
	if !errors.Is(err, ember.ErrUnsupportedFilter) {
		t.Errorf("got %v, want ErrUnsupportedFilter", err)
	}
}
