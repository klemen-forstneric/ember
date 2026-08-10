package postgres

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

// A Save invoked inside WithinTx must execute on the transaction (between Begin
// and Commit), not directly on the db.
func TestEntitySaveJoinsTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO entities").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	pg := NewDB(db)
	repo := NewEntityRepository(pg, "entities")
	err = pg.WithinTx(context.Background(), func(ctx context.Context) error {
		return repo.Save(ctx, &ember.MarshaledEntity{ID: "1", Type: "order", Version: ember.NewVersion(1), Data: []byte(`{}`)})
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A version conflict (0 rows affected) surfaces as ember.ErrVersionConflict.
func TestEntitySaveVersionConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("INSERT INTO entities").WillReturnResult(sqlmock.NewResult(0, 0))

	repo := NewEntityRepository(NewDB(db), "entities")
	err = repo.Save(context.Background(), &ember.MarshaledEntity{ID: "1", Type: "order", Version: ember.NewVersion(2), Data: []byte(`{}`)})

	require.ErrorIs(t, err, ember.ErrVersionConflict)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetScansDocumentColumnsInOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT id, type, version, data FROM entities").
		WithArgs("1", "order").
		WillReturnRows(sqlmock.NewRows(documentColumns).AddRow("1", "order", 7, []byte(`{"n":"a"}`)))

	repo := NewEntityRepository(NewDB(db), "entities")
	got, err := repo.Get(context.Background(), "order", "1")

	require.NoError(t, err)
	require.Equal(t, "1", got.ID)
	require.Equal(t, "order", got.Type)
	require.Equal(t, uint64(7), got.Version.Value())
	require.JSONEq(t, `{"n":"a"}`, string(got.Data))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetMissingIsNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT id, type, version, data FROM entities").
		WillReturnRows(sqlmock.NewRows(documentColumns))

	repo := NewEntityRepository(NewDB(db), "entities")
	_, err = repo.Get(context.Background(), "order", "nope")

	require.ErrorIs(t, err, ember.ErrEntityNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}
