package wal

import (
	"context"
	"fmt"

	"github.com/klemen-forstneric/ember/postgres"
)

// EnsurePublication creates the publication if it does not already exist. Run
// it at startup over the ordinary pool — the relay only holds a replication
// connection. The publication needs no tables: pgoutput requires the name, but
// logical decoding messages are not filtered by table membership.
//
// name is interpolated, not bound: CREATE PUBLICATION takes an identifier, not
// a parameter. Pass a config-derived name, never user input.
func EnsurePublication(ctx context.Context, db *postgres.DB, name string) error {
	_, err := db.Conn(ctx).ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION %s", pgQuoteIdent(name)))
	if err != nil && !IsDuplicateObject(err) {
		return err
	}
	return nil
}

// pgQuoteIdent double-quotes an identifier, escaping embedded quotes.
func pgQuoteIdent(s string) string {
	out := make([]rune, 0, len(s)+2)
	out = append(out, '"')
	for _, r := range s {
		if r == '"' {
			out = append(out, '"')
		}
		out = append(out, r)
	}
	return string(append(out, '"'))
}
