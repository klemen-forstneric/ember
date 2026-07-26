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

// IsUndefinedObject reports SQLSTATE 42704: the slot does not exist yet.
func IsUndefinedObject(err error) bool { return code(err) == "42704" }

// IsSlotInvalidated reports SQLSTATE 55000, which START_REPLICATION returns
// when the slot can no longer supply changes — typically because
// max_slot_wal_keep_size discarded the WAL it needed. Everything after the
// slot's confirmed_flush_lsn is gone; there is no outbox table to recover from.
func IsSlotInvalidated(err error) bool { return code(err) == "55000" }
