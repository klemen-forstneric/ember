package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"hash/fnv"
	"io/fs"
	"strconv"
	"strings"
)

// Migrate applies consecutive NNN_name.sql files in the root of source. All
// pending migrations and their checksums commit together. Concurrent callers
// serialize on a transaction-scoped advisory lock for the ledger name. Applied
// files are immutable; schema downgrades and gaps are rejected. SQL requiring
// execution outside a transaction (e.g. concurrent indexes) needs separate tooling.
func Migrate(ctx context.Context, pool *sql.DB, ledger string, source fs.FS) error {
	table, err := quotedIdentifier(ledger)
	if err != nil {
		return err
	}
	names, err := fs.Glob(source, "*.sql")
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return fmt.Errorf("postgres: no SQL migrations")
	}
	// Glob sorts lexically; require fixed-width, contiguous numeric prefixes.
	for i, name := range names {
		prefix, _, ok := strings.Cut(name, "_")
		n, err := strconv.Atoi(prefix)
		if !ok || err != nil || len(prefix) != 3 || n != i+1 {
			return fmt.Errorf("postgres: expected migration %03d_name.sql, got %q", i+1, name)
		}
	}
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(ledger))
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(hash.Sum64())); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+table+` (
		version bigint PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT version, checksum FROM `+table+` ORDER BY version`)
	if err != nil {
		return err
	}
	var checksums []string
	for rows.Next() {
		var version int
		var sum string
		if err = rows.Scan(&version, &sum); err != nil {
			rows.Close()
			return err
		}
		if version != len(checksums)+1 || version > len(names) {
			rows.Close()
			return fmt.Errorf("postgres: migration ledger has a gap or is newer than this binary at version %d", version)
		}
		checksums = append(checksums, sum)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for i, name := range names {
		raw, err := fs.ReadFile(source, name)
		if err != nil {
			return err
		}
		sum := fmt.Sprintf("%x", sha256.Sum256(raw))
		if i < len(checksums) {
			if checksums[i] != sum {
				return fmt.Errorf("postgres: migration %s checksum changed", name)
			}
			continue
		}
		if _, err = tx.ExecContext(ctx, string(raw)); err != nil {
			return fmt.Errorf("postgres: apply %s: %w", name, err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+table+` (version,checksum) VALUES ($1,$2)`, i+1, sum); err != nil {
			return err
		}
	}
	return tx.Commit()
}
