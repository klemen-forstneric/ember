package postgres

import (
	"context"
	"database/sql"

	"github.com/klemen-forstneric/ember"
)

type txKey struct{}

func ctxWithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func txFromCtx(ctx context.Context) *sql.Tx {
	tx, _ := ctx.Value(txKey{}).(*sql.Tx)
	return tx
}

// querier is the subset of *sql.DB / *sql.Tx the repositories use, so a write
// can run on whichever is active on the ctx.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func querierFrom(ctx context.Context, db *sql.DB) querier {
	if tx := txFromCtx(ctx); tx != nil {
		return tx
	}
	return db
}

// Transactor runs work inside a database/sql transaction. Reentrant: if the ctx
// already carries a *sql.Tx it joins that transaction rather than beginning a
// nested one (database/sql has no nested transactions).
type Transactor struct {
	db *sql.DB
}

func NewTransactor(db *sql.DB) *Transactor {
	return &Transactor{db: db}
}

var _ ember.Transactor = (*Transactor)(nil)

func (t *Transactor) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if txFromCtx(ctx) != nil {
		return fn(ctx)
	}

	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := fn(ctxWithTx(ctx, tx)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
