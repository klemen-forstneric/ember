package mongo

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/klemen-forstneric/ember"
)

func makeEntity(n, id string) bson.D {
	return bson.D{
		{Key: "entity_id", Value: id},
		{Key: "type", Value: "fake"},
		{Key: "version", Value: uint64(1)},
		{Key: "data", Value: bson.D{{Key: "n", Value: n}}},
	}
}

func nValues(ms []*ember.MarshaledEntity) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		var d map[string]string
		_ = json.Unmarshal(m.Data, &d)
		out[i] = d["n"]
	}
	return out
}

func TestListSortAscendingDescending(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	docs := []interface{}{
		makeEntity("1", "id1"),
		makeEntity("3", "id3"),
		makeEntity("2", "id2"),
	}
	_, err := col.InsertMany(ctx, docs)
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	asc, err := repo.List(ctx, "fake", nil, ember.Asc("n"), ember.Unpaged())
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2", "3"}, nValues(asc))

	desc, err := repo.List(ctx, "fake", nil, ember.Desc("n"), ember.Unpaged())
	require.NoError(t, err)
	require.Equal(t, []string{"3", "2", "1"}, nValues(desc))

	all, err := repo.List(ctx, "fake", nil, ember.Sort{}, ember.Unpaged())
	require.NoError(t, err)
	require.Len(t, all, 3)
}

func makeNumEntity(n int, id string) bson.D {
	return bson.D{
		{Key: "entity_id", Value: id},
		{Key: "type", Value: "fake"},
		{Key: "version", Value: uint64(1)},
		{Key: "data", Value: bson.D{{Key: "n", Value: n}}},
	}
}

func nNumbers(ms []*ember.MarshaledEntity) []float64 {
	out := make([]float64, len(ms))
	for i, m := range ms {
		var d map[string]float64
		_ = json.Unmarshal(m.Data, &d)
		out[i] = d["n"]
	}
	return out
}

func TestListSortNumeric(t *testing.T) {
	col := connectTestMongo(t)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []interface{}{
		makeNumEntity(9, "id9"),
		makeNumEntity(10, "id10"),
		makeNumEntity(2, "id2"),
	})
	require.NoError(t, err)

	repo, err := NewEntityRepository(ctx, col)
	require.NoError(t, err)

	asc, err := repo.List(ctx, "fake", nil, ember.Asc("n").Numeric(), ember.Unpaged())
	require.NoError(t, err)
	require.Equal(t, []float64{2, 9, 10}, nNumbers(asc))

	desc, err := repo.List(ctx, "fake", nil, ember.Desc("n").Numeric(), ember.Unpaged())
	require.NoError(t, err)
	require.Equal(t, []float64{10, 9, 2}, nNumbers(desc))
}
