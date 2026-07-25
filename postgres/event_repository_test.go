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
	rows := sqlmock.NewRows([]string{"id", "entity_id", "type", "data", "metadata", "created_at"}).
		AddRow("e1", "A", "Created", []byte(`{"k":"v"}`), []byte(`{"corr":"c-e1"}`), ts)
	mock.ExpectQuery("SELECT .* FROM events").WillReturnRows(rows)

	repo := NewEventRepository(NewDB(db), "events")
	got, err := repo.ListUnpublished(context.Background(), 10)

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "e1", got[0].ID)
	require.Equal(t, "A", got[0].EntityID)
	require.Equal(t, "Created", got[0].Event.Type)
	require.JSONEq(t, `{"k":"v"}`, string(got[0].Event.Data))
	require.Equal(t, ts, got[0].Timestamp)
	require.Equal(t, "c-e1", got[0].Metadata[ember.MetadataKey("corr")])
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

// Compile-time assertion that the repository satisfies the interface.
var _ ember.EventRepository = (*EventRepository)(nil)
