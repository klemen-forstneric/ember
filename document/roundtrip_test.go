package document_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/document"
	"github.com/klemen-forstneric/ember/embertest"
	"github.com/stretchr/testify/require"
)

type item struct {
	ember.EntityRoot
	Name  string
	Score int
}

func (*item) Type() string { return "item" }

type itemData struct {
	ID       string `json:"id"`
	Version  uint64 `json:"version"`
	Revision int    `json:"revision"`
	Name     string `json:"name"`
	Score    int    `json:"score"`
}
type itemMarshaler struct{}

func (itemMarshaler) Marshal(_ context.Context, e *item) (*ember.MarshaledEntity, error) {
	raw, err := json.Marshal(itemData{ID: e.ID(), Version: e.Version().Value(), Revision: 1, Name: e.Name, Score: e.Score})
	return &ember.MarshaledEntity{Data: raw}, err
}
func (itemMarshaler) Unmarshal(_ context.Context, m *ember.MarshaledEntity) (*item, error) {
	raw, err := ember.Migrate(m.Data, []ember.Migration{func(raw []byte) ([]byte, error) {
		var old map[string]any
		if err := json.Unmarshal(raw, &old); err != nil {
			return nil, err
		}
		old["name"] = old["label"]
		old["revision"] = 1
		delete(old, "label")
		return json.Marshal(old)
	}})
	if err != nil {
		return nil, err
	}
	var data itemData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	e := &item{EntityRoot: ember.NewEntityRoot(m.ID), Name: data.Name, Score: data.Score}
	e.SetVersion(m.Version)
	return e, nil
}
func TestDocumentSaveMigrationAndPagination(t *testing.T) {
	ctx := context.Background()
	repo := embertest.New()
	adapter := document.NewAdapter(repo, itemMarshaler{})
	saver := ember.NewEntitySaver(embertest.NewRecorder().Publisher(), ember.NopTransactor, nil, ember.Bind(adapter))
	for _, row := range []struct {
		id    string
		score int
	}{{"a", 2}, {"b", 2}, {"c", 10}} {
		e := &item{EntityRoot: ember.NewEntityRoot(row.id), Name: "current", Score: row.score}
		require.NoError(t, saver.Save(ctx, e))
		require.Equal(t, uint64(1), e.Version().Value())
		stored, err := repo.Get(ctx, "item", row.id)
		require.NoError(t, err)
		var data itemData
		require.NoError(t, json.Unmarshal(stored.Data, &data))
		require.Equal(t, uint64(1), data.Version)
	}
	page, err := adapter.ListPage(ctx, ember.Eq("name", "current"), ember.AscNum("score"), ember.Limit(1))
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, "a", page[0].ID())
	page, err = adapter.ListPage(ctx, ember.Eq("name", "current"), ember.AscNum("score"), ember.Limit(2).After(2, "a"))
	require.NoError(t, err)
	require.Len(t, page, 2)
	require.Equal(t, "b", page[0].ID())
	require.Equal(t, "c", page[1].ID())
	stale, err := adapter.Get(ctx, "a")
	require.NoError(t, err)
	current, err := adapter.Get(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, saver.Save(ctx, current))
	require.ErrorIs(t, saver.Save(ctx, stale), ember.ErrVersionConflict)
	require.Equal(t, uint64(1), stale.Version().Value())
	require.NoError(t, repo.Save(ctx, &ember.MarshaledEntity{ID: "legacy", Type: "item", Version: ember.NewVersion(0).Inc(), Data: []byte(`{"label":"old name"}`)}))
	migrated, err := adapter.Get(ctx, "legacy")
	require.NoError(t, err)
	require.Equal(t, "old name", migrated.Name)
}
