package ember

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortConstructors(t *testing.T) {
	assert.Equal(t, Sort{Path: "created_at", Direction: Ascending}, Asc("created_at"))
	assert.Equal(t, Sort{Path: "created_at", Direction: Descending}, Desc("created_at"))
	assert.Equal(t, "", Sort{}.Path)
	assert.Equal(t, Sort{}, Unsorted())
	assert.Equal(t, "", Unsorted().Path)
}

func TestSortOrderingConstructors(t *testing.T) {
	assert.Equal(t, Sort{Path: "created_at", Direction: Ascending, Ordering: Lexical}, AscLex("created_at"))
	assert.Equal(t, Sort{Path: "created_at", Direction: Descending, Ordering: Lexical}, DescLex("created_at"))
	assert.Equal(t, Sort{Path: "seq", Direction: Ascending, Ordering: Numeric}, AscNum("seq"))
	assert.Equal(t, Sort{Path: "seq", Direction: Descending, Ordering: Numeric}, DescNum("seq"))
}

func TestSortValidate(t *testing.T) {
	require.NoError(t, Unsorted().Validate())
	require.NoError(t, AscNum("seq").Validate())
	require.NoError(t, DescLex("created_at").Validate())

	require.NoError(t, Asc("id").Validate())
	require.NoError(t, Asc("type").Validate())
	require.NoError(t, Desc("version").Validate())

	require.ErrorIs(t, Asc("seq").Validate(), ErrInvalidSort)
	require.ErrorIs(t, Desc("created_at").Validate(), ErrInvalidSort)
	require.ErrorIs(t, Asc("job.id").Validate(), ErrInvalidSort)
}

func TestSortOrderingIsUndeclaredByDefault(t *testing.T) {
	assert.NotEqual(t, Lexical, Asc("seq").Ordering)
	assert.NotEqual(t, Numeric, Asc("seq").Ordering)
	assert.Equal(t, Lexical, AscLex("seq").Ordering)
	assert.Equal(t, Numeric, AscNum("seq").Ordering)
}
