package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/require"
)

type mapperMethod string
type mapperCount int64

type mapperEntity struct {
	ember.EntityRoot
	Email     string
	Method    mapperMethod
	Count     mapperCount
	Optional  *mapperMethod
	CreatedAt time.Time
	Verified  *time.Time
	Transient string
}

func (*mapperEntity) Type() string { return "mapper_entity" }

type mapperRow struct {
	Address   string     `db:"email" entity:"Email"`
	Method    string     `db:"method"`
	Count     int64      `db:"count"`
	Optional  *string    `db:"optional"`
	CreatedAt time.Time  `db:"created_at"`
	Verified  *time.Time `db:"verified"`
	Ignored   string     `db:"-"`
}

func TestStructMapperCopiesFieldsEnumsAndMetadata(t *testing.T) {
	mapper := &StructMapper[*mapperEntity, mapperRow]{}
	// Construction validates the mapping without accessing a database.
	_, err := NewRepository(nil, "mapped_entities", mapper)
	require.NoError(t, err)
	at := time.Now().UTC()
	method := mapperMethod("password")
	e := &mapperEntity{EntityRoot: ember.NewEntityRoot("one"), Email: "a@example.test",
		Method: method, Count: 42, Optional: &method, CreatedAt: at, Verified: &at, Transient: "in memory"}
	e.SetVersion(ember.NewVersion(7))
	row, err := mapper.Marshal(context.Background(), e)
	require.NoError(t, err)
	require.Equal(t, e.Email, row.Address)
	require.Equal(t, "password", row.Method)
	require.Equal(t, int64(42), row.Count)
	require.Equal(t, "password", *row.Optional)
	require.Equal(t, at, *row.Verified)
	require.Empty(t, row.Ignored)
	// Mutating either representation must not change the other through a pointer.
	*row.Optional = "google"
	*row.Verified = at.Add(time.Hour)
	require.Equal(t, mapperMethod("password"), *e.Optional)
	require.Equal(t, at, *e.Verified)
	got, err := mapper.Unmarshal(context.Background(), Row[mapperRow]{ID: "stored", Version: ember.NewVersion(8), Data: row})
	require.NoError(t, err)
	require.Equal(t, "stored", got.ID())
	require.Equal(t, uint64(8), got.Version().Value())
	require.Equal(t, mapperMethod("password"), got.Method)
	require.Equal(t, mapperCount(42), got.Count)
	require.Equal(t, mapperMethod("google"), *got.Optional)
	require.Empty(t, got.Transient)
	*got.Optional = "other"
	*got.Verified = at
	require.Equal(t, "google", *row.Optional)
	require.Equal(t, at.Add(time.Hour), *row.Verified)
	require.Equal(t, uint64(7), e.Version().Value())
}

func TestStructMapperPreservesNilAndRejectsNilEntity(t *testing.T) {
	mapper := &StructMapper[*mapperEntity, mapperRow]{}
	row, err := mapper.Marshal(context.Background(), &mapperEntity{EntityRoot: ember.NewEntityRoot("one")})
	require.NoError(t, err)
	require.Nil(t, row.Optional)
	require.Nil(t, row.Verified)
	got, err := mapper.Unmarshal(context.Background(), Row[mapperRow]{ID: "one", Data: row})
	require.NoError(t, err)
	require.Nil(t, got.Optional)
	require.Nil(t, got.Verified)
	_, err = mapper.Marshal(context.Background(), nil)
	require.ErrorContains(t, err, "nil entity")
}

func TestStructMapperRejectsInvalidMappingsAtRepositoryConstruction(t *testing.T) {
	type missingField struct {
		Missing string `db:"missing"`
	}
	type narrowedNumber struct {
		Count int32 `db:"count"`
	}
	type numberToString struct {
		Count string `db:"count"`
	}
	type pointerMismatch struct {
		Email *string `db:"email"`
	}
	type duplicateTarget struct {
		First  string `db:"first" entity:"Email"`
		Second string `db:"second" entity:"Email"`
	}
	check := func(err error) { t.Helper(); require.Error(t, err) }
	_, err := NewRepository(nil, "items", &StructMapper[*mapperEntity, missingField]{})
	check(err)
	_, err = NewRepository(nil, "items", &StructMapper[*mapperEntity, narrowedNumber]{})
	check(err)
	_, err = NewRepository(nil, "items", &StructMapper[*mapperEntity, numberToString]{})
	check(err)
	_, err = NewRepository(nil, "items", &StructMapper[*mapperEntity, pointerMismatch]{})
	check(err)
	_, err = NewRepository(nil, "items", &StructMapper[*mapperEntity, duplicateTarget]{})
	check(err)
	_, err = NewRepository(nil, "items", &StructMapper[ember.Entity, mapperRow]{})
	check(err)
	// NativeItem's private owner field is supported by its custom mapper, but is
	// deliberately rejected by the automatic mapper instead of silently omitted.
	type privateField struct {
		Owner string `db:"owner" entity:"owner"`
	}
	_, err = NewRepository(nil, "items", &StructMapper[*nativeItem, privateField]{})
	check(err)
}

func TestStructMapperCanBeSharedConcurrently(t *testing.T) {
	mapper := &StructMapper[*mapperEntity, mapperRow]{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := &mapperEntity{EntityRoot: ember.NewEntityRoot("one"), Email: "a@example.test"}
			row, err := mapper.Marshal(context.Background(), e)
			if err != nil {
				t.Error(err)
				return
			}
			got, err := mapper.Unmarshal(context.Background(), Row[mapperRow]{ID: e.ID(), Data: row})
			if err != nil {
				t.Error(err)
				return
			}
			if got.Email != e.Email {
				t.Errorf("email: got %q, want %q", got.Email, e.Email)
			}
		}()
	}
	wg.Wait()
}
