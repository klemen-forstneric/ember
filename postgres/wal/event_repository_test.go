package wal

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/klemen-forstneric/ember"
	"github.com/klemen-forstneric/ember/postgres"
)

func env(id string) ember.EventEnvelope {
	return ember.EventEnvelope{
		ID:        id,
		EntityID:  "A",
		Event:     &ember.MarshaledEvent{Type: "Created", Data: []byte(`{"k":"v"}`)},
		Metadata:  ember.Metadata{ember.MetadataKey("correlation_id"): "c-" + id},
		Timestamp: time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC),
	}
}

func TestSaveEmitsOneMessagePerEnvelope(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	for _, id := range []string{"e1", "e2"} {
		payload, err := encode(env(id))
		require.NoError(t, err)
		mock.ExpectExec("SELECT pg_logical_emit_message").
			WithArgs("svc_events", payload).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	repo := NewEventRepository(postgres.NewDB(db), "svc_events")
	require.NoError(t, repo.Save(context.Background(), []ember.EventEnvelope{env("e1"), env("e2")}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSaveNoEnvelopesIsNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	repo := NewEventRepository(postgres.NewDB(db), "svc_events")
	require.NoError(t, repo.Save(context.Background(), nil))
	require.NoError(t, mock.ExpectationsWereMet())
}

var _ ember.EventRepository = (*EventRepository)(nil)
