package ember

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func addField(key, val string) Migration {
	return func(data []byte) ([]byte, error) {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		m[key] = val
		return json.Marshal(m)
	}
}

func TestMigrateBaselineNoRevisionRunsAll(t *testing.T) {
	// No revision field present -> stored 0 -> both migrations run.
	got, err := Migrate([]byte(`{"name":"x"}`), []Migration{addField("a", "1"), addField("b", "2")})
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(got, &m))
	require.Equal(t, "1", m["a"])
	require.Equal(t, "2", m["b"])
}

func TestMigratePartialFromStored(t *testing.T) {
	// Stored revision 1 -> only migrations[1:] run.
	got, err := Migrate([]byte(`{"revision":1}`), []Migration{addField("a", "1"), addField("b", "2")})
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(got, &m))
	require.Nil(t, m["a"])
	require.Equal(t, "2", m["b"])
}

func TestMigrateAlreadyCurrentIsNoop(t *testing.T) {
	in := []byte(`{"revision":2,"keep":true}`)
	got, err := Migrate(in, []Migration{addField("a", "1"), addField("b", "2")})
	require.NoError(t, err)
	require.Equal(t, in, got)
}

func TestMigrateRevisionAhead(t *testing.T) {
	_, err := Migrate([]byte(`{"revision":5}`), []Migration{addField("a", "1")})
	require.ErrorIs(t, err, ErrRevisionAhead)
}

func TestMigratePropagatesError(t *testing.T) {
	boom := func([]byte) ([]byte, error) { return nil, errors.New("boom") }
	_, err := Migrate([]byte(`{}`), []Migration{boom})
	require.Error(t, err)
	require.Contains(t, err.Error(), "0->1")
}

func TestMigrateNoMigrations(t *testing.T) {
	in := []byte(`{"x":1}`)
	got, err := Migrate(in, nil)
	require.NoError(t, err)
	require.Equal(t, in, got)
}

func TestMigrateNegativeRevisionTreatedAsBaseline(t *testing.T) {
	got, err := Migrate([]byte(`{"revision":-1}`), []Migration{addField("a", "1")})
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(got, &m))
	require.Equal(t, "1", m["a"])
}
