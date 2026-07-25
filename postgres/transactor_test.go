package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestWithinTxCommitsOnSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	pg := NewDB(db)
	err = pg.WithinTx(context.Background(), func(ctx context.Context) error {
		_, err := pg.Conn(ctx).ExecContext(ctx, "INSERT INTO t VALUES (1)")
		return err
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxRollsBackOnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	boom := errors.New("boom")
	mock.ExpectBegin()
	mock.ExpectRollback()

	pg := NewDB(db)
	err = pg.WithinTx(context.Background(), func(ctx context.Context) error { return boom })

	require.ErrorIs(t, err, boom)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxReentrantJoinsExistingTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// One begin (ours), the fn's exec, one commit (ours) — the reentrant
	// WithinTx must NOT begin a second transaction.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	outer, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), txKey{}, outer)

	pg := NewDB(db)
	ran := false
	err = pg.WithinTx(ctx, func(ctx context.Context) error {
		ran = true
		require.NotNil(t, ctx.Value(txKey{}), "fn keeps the existing tx on ctx")
		_, e := pg.Conn(ctx).ExecContext(ctx, "INSERT INTO t VALUES (1)")
		return e
	})
	require.NoError(t, err)
	require.True(t, ran)
	require.NoError(t, outer.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxBeginError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	boom := errors.New("begin boom")
	mock.ExpectBegin().WillReturnError(boom)

	pg := NewDB(db)
	err = pg.WithinTx(context.Background(), func(ctx context.Context) error { return nil })

	require.ErrorIs(t, err, boom)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Conn returns the ctx transaction when one is active, else the pool.
func TestConn(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	pg := NewDB(db)

	// no tx on ctx -> returns the pool
	require.Equal(t, conn(db), pg.Conn(context.Background()))

	// tx on ctx -> returns that tx, not the pool
	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)
	require.Equal(t, conn(tx), pg.Conn(context.WithValue(context.Background(), txKey{}, tx)))

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}
