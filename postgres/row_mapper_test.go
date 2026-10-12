package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/klemen-forstneric/ember"
	"github.com/stretchr/testify/require"
)

type mapperFuncs struct {
	marshal   func(context.Context, *nativeItem) (nativeRow, error)
	unmarshal func(context.Context, Row[nativeRow]) (*nativeItem, error)
}

func (m mapperFuncs) Marshal(ctx context.Context, e *nativeItem) (nativeRow, error) {
	if m.marshal != nil {
		return m.marshal(ctx, e)
	}
	return (nativeMapper{}).Marshal(ctx, e)
}

func (m mapperFuncs) Unmarshal(ctx context.Context, row Row[nativeRow]) (*nativeItem, error) {
	if m.unmarshal != nil {
		return m.unmarshal(ctx, row)
	}
	return (nativeMapper{}).Unmarshal(ctx, row)
}

type mapperContextKey struct{}

func TestRowMarshalErrorDoesNotWriteOrAdvanceEntity(t *testing.T) {
	pool, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer pool.Close()
	failure := errors.New("cannot represent domain value")
	ctx := context.WithValue(context.Background(), mapperContextKey{}, "conversion context")
	e := &nativeItem{EntityRoot: ember.NewEntityRoot("one"), secret: "domain-only state"}
	r, err := NewRepository(NewDB(pool), "items", mapperFuncs{
		marshal: func(got context.Context, entity *nativeItem) (nativeRow, error) {
			require.Equal(t, "conversion context", got.Value(mapperContextKey{}))
			require.Same(t, e, entity)
			require.Zero(t, entity.Version().Value())
			return nativeRow{}, failure
		},
	})
	require.NoError(t, err)
	require.ErrorIs(t, r.Save(ctx, e, e.Version().Inc()), failure)
	require.Zero(t, e.Version().Value())
	require.Equal(t, "domain-only state", e.secret)
	// No SQL expectations: conversion must succeed before any database write.
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRowUnmarshalReceivesMetadataAndContext(t *testing.T) {
	pool, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer pool.Close()
	mock.ExpectQuery("SELECT").WithArgs("one").WillReturnRows(mapperTestRows())
	ctx := context.WithValue(context.Background(), mapperContextKey{}, "read context")
	r, err := NewRepository(NewDB(pool), "items", mapperFuncs{
		unmarshal: func(got context.Context, row Row[nativeRow]) (*nativeItem, error) {
			require.Equal(t, "read context", got.Value(mapperContextKey{}))
			require.Equal(t, "one", row.ID)
			require.Equal(t, uint64(7), row.Version.Value())
			require.Equal(t, "stored owner", row.Data.Account)
			e, err := (nativeMapper{}).Unmarshal(got, row)
			// The repository, not the mapper, owns the persisted version.
			e.SetVersion(ember.NewVersion(99))
			return e, err
		},
	})
	require.NoError(t, err)
	e, err := r.Get(ctx, "one")
	require.NoError(t, err)
	require.Equal(t, "stored owner", e.owner)
	require.Equal(t, uint64(7), e.Version().Value())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRowUnmarshalFailuresPropagateThroughGetAndList(t *testing.T) {
	failure := errors.New("unsupported stored representation")
	for _, mode := range []string{"conversion", "nil", "identity"} {
		t.Run(mode, func(t *testing.T) {
			pool, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer pool.Close()
			r, err := NewRepository(NewDB(pool), "items", mapperFuncs{
				unmarshal: func(context.Context, Row[nativeRow]) (*nativeItem, error) {
					switch mode {
					case "conversion":
						return nil, failure
					case "identity":
						return &nativeItem{EntityRoot: ember.NewEntityRoot("wrong")}, nil
					default:
						return nil, nil
					}
				},
			})
			require.NoError(t, err)
			for _, list := range []bool{false, true} {
				mock.ExpectQuery("SELECT").WillReturnRows(mapperTestRows()).RowsWillBeClosed()
				if list {
					var entities []*nativeItem
					entities, err = r.ListPage(context.Background(), nil, ember.Unsorted(), ember.Limit(10))
					require.Nil(t, entities)
				} else {
					var entity *nativeItem
					entity, err = r.Get(context.Background(), "one")
					require.Nil(t, entity)
				}
				switch mode {
				case "conversion":
					require.ErrorIs(t, err, failure)
				case "identity":
					require.ErrorContains(t, err, "changed identity")
				case "nil":
					require.ErrorContains(t, err, "nil entity")
				}
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func mapperTestRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "version", "owner", "rank", "active", "state", "optional", "alias", "at", "later"}).
		AddRow("one", 7, "stored owner", 2, true, "open", nil, nil, time.Now().UTC(), nil)
}
