package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
)

func env(id string, ts time.Time) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:        id,
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("corr"): "c-" + id},
		Timestamp: ts,
	}
}

func TestEventSaveInsertsUnpublished(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("INSERT INTO events").WillReturnResult(sqlmock.NewResult(0, 2))

	repo := NewEventRepository(NewDB(db), "events")
	err = repo.Save(context.Background(), []ember.EventEnvelope{
		env("e1", time.Unix(1, 0).UTC()),
		env("e2", time.Unix(2, 0).UTC()),
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventSaveEmptyIsNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	// No expectations: Save with no envelopes must issue no query.

	repo := NewEventRepository(NewDB(db), "events")
	require.NoError(t, repo.Save(context.Background(), nil))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventListUnpublishedMapsRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ts := time.Unix(1_700_000_000, 0).UTC()
	rows := sqlmock.NewRows([]string{"id", "entity_id", "type", "data", "metadata", "version", "idx", "created_at"}).
		AddRow("e1", "A", "Created", []byte(`{"k":"v"}`), []byte(`{"corr":"c-e1"}`), int64(1), 0, ts)
	mock.ExpectQuery("SELECT id, entity_id, type, data, metadata, version, idx, created_at FROM events " +
		"WHERE NOT published ORDER BY entity_id, version, idx LIMIT 10").
		WillReturnRows(rows)

	repo := NewEventRepository(NewDB(db), "events")
	got, err := repo.ListUnpublished(context.Background(), 10)

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "e1", got[0].ID)
	require.Equal(t, "A", got[0].EntityID)
	require.Equal(t, "Created", got[0].Event.Type)
	require.JSONEq(t, `{"k":"v"}`, string(got[0].Event.Data))
	require.Equal(t, uint64(1), got[0].Version)
	require.Equal(t, 0, got[0].Index)
	require.Equal(t, ts, got[0].Timestamp)
	require.Equal(t, "c-e1", got[0].Metadata[ember.MetadataKey("corr")])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventListUnpublishedNoLimitOmitsLimitClause(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "entity_id", "type", "data", "metadata", "version", "idx", "created_at"})
	mock.ExpectQuery("SELECT id, entity_id, type, data, metadata, version, idx, created_at FROM events " +
		"WHERE NOT published ORDER BY entity_id, version, idx$").
		WillReturnRows(rows)

	repo := NewEventRepository(NewDB(db), "events")
	_, err = repo.ListUnpublished(context.Background(), 0)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventMarkPublished(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("UPDATE events").WillReturnResult(sqlmock.NewResult(0, 2))

	repo := NewEventRepository(NewDB(db), "events")
	err = repo.MarkPublished(context.Background(), []string{"e1", "e2"}, time.Unix(9, 0).UTC())

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventMarkPublishedEmptyIsNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := NewEventRepository(NewDB(db), "events")
	require.NoError(t, repo.MarkPublished(context.Background(), nil, time.Now()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventListUnpublishedOrdersByEntityThenVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "entity_id", "type", "data", "metadata", "version", "idx", "created_at"}).
		AddRow("e1", "A", "Created", []byte(`{"k":"v"}`), []byte(`{"corr":"c-e1"}`), int64(1), 0, time.Unix(1, 0).UTC()).
		AddRow("e2", "A", "Created", []byte(`{"k":"v"}`), []byte(`{"corr":"c-e2"}`), int64(1), 1, time.Unix(1, 0).UTC())

	mock.ExpectQuery("SELECT id, entity_id, type, data, metadata, version, idx, created_at FROM events " +
		"WHERE NOT published ORDER BY entity_id, version, idx LIMIT 3").
		WillReturnRows(rows)

	repo := NewEventRepository(NewDB(db), "events")
	got, err := repo.ListUnpublished(context.Background(), 3)

	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, uint64(1), got[0].Version)
	require.Equal(t, 1, got[1].Index)
	require.Equal(t, "A", got[0].EntityID)
	require.Equal(t, []byte(`{"k":"v"}`), got[0].Event.Data)
	require.Equal(t, "c-e1", got[0].Metadata[ember.MetadataKey("corr")])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEventSaveWritesVersionAndIdx(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	e := env("e1", time.Unix(1, 0).UTC())
	e.Version = 7
	e.Index = 2

	mock.ExpectExec("INSERT INTO events").
		WithArgs("e1", "A", "Created", []byte(`{"k":"v"}`), sqlmock.AnyArg(), int64(7), 2, e.Timestamp, false).
		WillReturnResult(sqlmock.NewResult(0, 1))

	repo := NewEventRepository(NewDB(db), "events")
	require.NoError(t, repo.Save(context.Background(), []ember.EventEnvelope{e}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// Compile-time assertion that the repository satisfies the interfaces.
var (
	_ ember.EventRepository        = (*EventRepository)(nil)
	_ ember.PollingRelayRepository = (*EventRepository)(nil)
)
