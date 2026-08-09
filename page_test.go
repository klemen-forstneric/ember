package ember

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageConstructors(t *testing.T) {
	assert.Equal(t, Page{}, Unpaged())
	assert.True(t, Unpaged().IsZero())
	assert.Equal(t, Page{Limit: 20}, Limit(20))
	assert.Equal(t, Page{Limit: 20, Offset: 40}, Limit(20).Skip(40))
	assert.Equal(t, Page{Limit: 20, Cursor: Cursor{Value: int64(7), ID: "x"}}, Limit(20).After(int64(7), "x"))
	assert.Equal(t, Page{Limit: 20, Cursor: Cursor{ID: "x"}}, Limit(20).After(nil, "x"))
	assert.Equal(t, Page{Limit: 20, Cursor: Cursor{Value: "a", ID: "x"}}, Limit(20).AfterCursor(Cursor{Value: "a", ID: "x"}))
	assert.False(t, Limit(20).IsZero())
}

func TestPageValidate(t *testing.T) {
	require.NoError(t, Unpaged().Validate())
	require.NoError(t, Limit(10).Validate())
	require.NoError(t, Limit(10).Skip(20).Validate())
	require.NoError(t, Limit(10).After(int64(1), "x").Validate())

	require.ErrorIs(t, Limit(-1).Validate(), ErrInvalidPage)
	require.ErrorIs(t, Page{Limit: 10, Offset: -1}.Validate(), ErrInvalidPage)
	require.ErrorIs(t, Limit(10).Skip(5).After(int64(1), "x").Validate(), ErrInvalidPage)
	require.ErrorIs(t, Page{Offset: 5}.Validate(), ErrInvalidPage)
	require.ErrorIs(t, Page{Cursor: Cursor{ID: "x"}}.Validate(), ErrInvalidPage)
	require.ErrorIs(t, Page{Limit: 10, Cursor: Cursor{Value: 7}}.Validate(), ErrInvalidPage)
}

func TestCursorIsZero(t *testing.T) {
	assert.True(t, Cursor{}.IsZero())
	assert.True(t, Cursor{Value: "a"}.IsZero())
	assert.False(t, Cursor{ID: "x"}.IsZero())
}

func TestCursorTextRoundTrip(t *testing.T) {
	ts := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		in   Cursor
		want Cursor
	}{
		{"unsorted", Cursor{ID: "pay_abc"}, Cursor{ID: "pay_abc"}},
		{"string", Cursor{Value: "2026-08-09", ID: "a"}, Cursor{Value: "2026-08-09", ID: "a"}},
		{"string with separator", Cursor{Value: "a|b|c", ID: "x|y"}, Cursor{Value: "a|b|c", ID: "x|y"}},
		{"int", Cursor{Value: 7, ID: "a"}, Cursor{Value: int64(7), ID: "a"}},
		{"int64 beyond float precision", Cursor{Value: int64(9007199254740993), ID: "a"}, Cursor{Value: int64(9007199254740993), ID: "a"}},
		{"float", Cursor{Value: 1.5, ID: "a"}, Cursor{Value: 1.5, ID: "a"}},
		{"integral float", Cursor{Value: float64(7), ID: "a"}, Cursor{Value: float64(7), ID: "a"}},
		{"float32", Cursor{Value: float32(1.5), ID: "a"}, Cursor{Value: float64(1.5), ID: "a"}},
		{"bool true", Cursor{Value: true, ID: "a"}, Cursor{Value: true, ID: "a"}},
		{"bool false", Cursor{Value: false, ID: "a"}, Cursor{Value: false, ID: "a"}},
		{"time", Cursor{Value: ts, ID: "a"}, Cursor{Value: ts.Format(time.RFC3339Nano), ID: "a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := tt.in.MarshalText()
			require.NoError(t, err)

			var got Cursor
			require.NoError(t, got.UnmarshalText(b))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCursorTextRejectsGarbage(t *testing.T) {
	var c Cursor
	require.ErrorIs(t, c.UnmarshalText([]byte("not base64 $$$")), ErrInvalidCursor)
}

func TestCursorZeroRoundTripsThroughEmpty(t *testing.T) {
	b, err := Cursor{}.MarshalText()
	require.NoError(t, err)
	assert.Empty(t, b)

	c := Cursor{Value: int64(7), ID: "x"}
	require.NoError(t, c.UnmarshalText(b))
	assert.Equal(t, Cursor{}, c)
}

func TestCursorMarshalRejectsUnsupportedValue(t *testing.T) {
	_, err := Cursor{Value: []string{"a"}, ID: "x"}.MarshalText()
	require.ErrorIs(t, err, ErrInvalidCursor)
}

func TestCursorMarshalRejectsValueWithoutID(t *testing.T) {
	_, err := Cursor{Value: "a"}.MarshalText()
	require.ErrorIs(t, err, ErrInvalidCursor)
}

func TestCursorMarshalRejectsUint64BeyondInt64(t *testing.T) {
	_, err := Cursor{Value: uint64(math.MaxInt64) + 1, ID: "x"}.MarshalText()
	require.ErrorIs(t, err, ErrInvalidCursor)
}

func TestCursorSurvivesJSON(t *testing.T) {
	type resp struct {
		Next Cursor `json:"next"`
	}

	b, err := json.Marshal(resp{Next: Cursor{Value: int64(42), ID: "pay_abc"}})
	require.NoError(t, err)

	var got resp
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, Cursor{Value: int64(42), ID: "pay_abc"}, got.Next)

	zero, err := json.Marshal(resp{})
	require.NoError(t, err)
	assert.JSONEq(t, `{"next":""}`, string(zero))

	back := resp{Next: Cursor{Value: int64(42), ID: "pay_abc"}}
	require.NoError(t, json.Unmarshal(zero, &back))
	assert.Equal(t, Cursor{}, back.Next)
}
