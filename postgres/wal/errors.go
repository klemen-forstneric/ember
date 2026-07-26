package wal

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

func code(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// IsDuplicateObject reports SQLSTATE 42710: the slot or publication already exists.
func IsDuplicateObject(err error) bool { return code(err) == "42710" }

// IsObjectInUse reports SQLSTATE 55006: another replica holds the slot.
func IsObjectInUse(err error) bool { return code(err) == "55006" }
