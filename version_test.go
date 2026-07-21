package ember

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionJSONRoundTrip(t *testing.T) {
	v := NewVersion(2).Inc().Inc() // value 4, delta 2

	data, err := json.Marshal(v)
	require.NoError(t, err)
	require.JSONEq(t, "4", string(data))

	var got Version
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, uint64(4), got.Value())
	require.Equal(t, uint64(4), got.Initial()) // delta collapsed on decode
}
