package wal

import (
	"github.com/jackc/pglogrepl"

	"github.com/klemen-forstneric/ember"
)

// decoder turns parsed replication messages into per-transaction batches, so a
// multi-event Save is delivered as the unit it was written as.
type decoder struct {
	prefix string
	inTx   bool
	buf    []ember.EventEnvelope
}

// pending reports that a transaction is open. The relay must not adopt a
// keepalive's ServerWALEnd while it is true, or it would advance past events
// this transaction has not delivered yet.
func (d *decoder) pending() bool { return d.inTx }

// apply feeds one message. ready is true only on commit, including a commit
// that produced no events for this prefix — the relay still advances past it.
func (d *decoder) apply(m pglogrepl.Message) ([]ember.EventEnvelope, pglogrepl.LSN, bool, error) {
	switch v := m.(type) {
	case *pglogrepl.BeginMessage:
		d.inTx = true
		d.buf = nil

	case *pglogrepl.LogicalDecodingMessage:
		if v.Prefix != d.prefix {
			return nil, 0, false, nil
		}
		e, err := decode(v.Content)
		if err != nil {
			return nil, 0, false, err
		}
		d.buf = append(d.buf, e)

	case *pglogrepl.CommitMessage:
		batch := d.buf
		d.buf, d.inTx = nil, false
		return batch, v.CommitLSN, true, nil
	}
	return nil, 0, false, nil
}
