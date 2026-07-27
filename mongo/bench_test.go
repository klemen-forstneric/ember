package mongo

import (
	"encoding/json"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/klemen-forstneric/ember"
)

// Payloads mirror what conversation-service actually stores.
var (
	memoryJSON = []byte(`{"id":"9f1c2b7a-3d4e-4f50-8a61-2c3d4e5f6071","version":3,"revision":1,"user_id":"6a50e5c1-1111-2222-3333-444455556666","agent_id":"b7e2d3c4-5555-6666-7777-888899990000","kind":"durable","content":"The user moved to Maribor for their new job and prefers tea over coffee.","category":"location","confidence":0.87,"importance":4,"conversation_id":"45d1924b-0b34-48a0-894a-198e21c172f2","status":"active","created_at":"2026-07-27T09:14:22.113Z","updated_at":"2026-07-27T09:14:22.113Z"}`)

	eventJSON = []byte(`{"memory_id":"9f1c2b7a-3d4e-4f50-8a61-2c3d4e5f6071","superseded_by":{"memory_id":"1a2b3c4d-5e6f-7081-92a3-b4c5d6e7f809","kind":"durable","category":"location","content":"The user moved to Maribor for their new job."}}`)

	// A summary after a long conversation: the largest thing either path carries.
	summaryJSON = append(append(
		[]byte(`{"id":"3c4d5e6f-7081-92a3-b4c5-d6e7f8091a2b","version":12,"revision":1,"conversation_id":"45d1924b-0b34-48a0-894a-198e21c172f2","user_id":"6a50e5c1-1111-2222-3333-444455556666","agent_id":"b7e2d3c4-5555-6666-7777-888899990000","seq":320,"content":"`),
		[]byte(repeat("The user discussed their relocation, work schedule, and dietary preferences at length. ", 40))...),
		[]byte(`","created_at":"2026-07-27T09:14:22.113Z","updated_at":"2026-07-27T09:14:22.113Z"}`)...)
)

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

func payloads() []struct {
	name string
	data []byte
} {
	return []struct {
		name string
		data []byte
	}{
		{"event-250B", eventJSON},
		{"memory-450B", memoryJSON},
		{"summary-3.5KB", summaryJSON},
	}
}

// BenchmarkDataToBSON is the write-side conversion: JSON payload to the stored
// document.
func BenchmarkDataToBSON(b *testing.B) {
	for _, p := range payloads() {
		b.Run(p.name, func(b *testing.B) {
			b.SetBytes(int64(len(p.data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var raw bson.Raw
				if err := bson.UnmarshalExtJSON(p.data, false, &raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDataToJSON is the read-side conversion: stored document back to the
// JSON the marshaler expects.
func BenchmarkDataToJSON(b *testing.B) {
	for _, p := range payloads() {
		var raw bson.Raw
		if err := bson.UnmarshalExtJSON(p.data, false, &raw); err != nil {
			b.Fatal(err)
		}
		b.Run(p.name, func(b *testing.B) {
			b.SetBytes(int64(len(p.data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := bson.MarshalExtJSON(raw, false, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEventEncode compares encoding one outbox row the old way (payload as
// bytes) against the new way (payload as a nested document).
func BenchmarkEventEncode(b *testing.B) {
	type oldEntry struct {
		ID        string         `bson:"_id"`
		EntityID  string         `bson:"entity_id"`
		Type      string         `bson:"type"`
		Data      []byte         `bson:"data"`
		Metadata  ember.Metadata `bson:"metadata"`
		Seq       int64          `bson:"seq"`
		CreatedAt time.Time      `bson:"created_at"`
		Published bool           `bson:"published"`
	}

	ts := time.Unix(1_700_000_000, 0).UTC()

	for _, p := range payloads() {
		b.Run(p.name+"/bytes", func(b *testing.B) {
			b.SetBytes(int64(len(p.data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				e := oldEntry{ID: "e1", EntityID: "A", Type: "T", Data: p.data, Seq: ts.UnixNano(), CreatedAt: ts}
				if _, err := bson.Marshal(e); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(p.name+"/document", func(b *testing.B) {
			b.SetBytes(int64(len(p.data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var raw bson.Raw
				if err := bson.UnmarshalExtJSON(p.data, false, &raw); err != nil {
					b.Fatal(err)
				}
				e := entry{ID: "e1", EntityID: "A", Type: "T", Data: raw, Seq: ts.UnixNano(), CreatedAt: ts}
				if _, err := bson.Marshal(e); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEventDecode is the relay's side of the same comparison.
func BenchmarkEventDecode(b *testing.B) {
	type oldEntry struct {
		ID        string         `bson:"_id"`
		EntityID  string         `bson:"entity_id"`
		Type      string         `bson:"type"`
		Data      []byte         `bson:"data"`
		Metadata  ember.Metadata `bson:"metadata"`
		Seq       int64          `bson:"seq"`
		CreatedAt time.Time      `bson:"created_at"`
		Published bool           `bson:"published"`
	}

	ts := time.Unix(1_700_000_000, 0).UTC()

	for _, p := range payloads() {
		oldDoc, err := bson.Marshal(oldEntry{ID: "e1", EntityID: "A", Type: "T", Data: p.data, Seq: ts.UnixNano(), CreatedAt: ts})
		if err != nil {
			b.Fatal(err)
		}

		var raw bson.Raw
		if err := bson.UnmarshalExtJSON(p.data, false, &raw); err != nil {
			b.Fatal(err)
		}
		newDoc, err := bson.Marshal(entry{ID: "e1", EntityID: "A", Type: "T", Data: raw, Seq: ts.UnixNano(), CreatedAt: ts})
		if err != nil {
			b.Fatal(err)
		}

		b.Run(p.name+"/bytes", func(b *testing.B) {
			b.SetBytes(int64(len(p.data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var d oldEntry
				if err := bson.Unmarshal(oldDoc, &d); err != nil {
					b.Fatal(err)
				}
				_ = d.Data
			}
		})

		b.Run(p.name+"/document", func(b *testing.B) {
			b.SetBytes(int64(len(p.data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var d entry
				if err := bson.Unmarshal(newDoc, &d); err != nil {
					b.Fatal(err)
				}
				if _, err := bson.MarshalExtJSON(d.Data, false, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkJSONBaseline is the encoding/json work the entity and event
// marshalers already do, for scale against the extended-JSON conversions.
func BenchmarkJSONBaseline(b *testing.B) {
	for _, p := range payloads() {
		b.Run(p.name+"/unmarshal", func(b *testing.B) {
			b.SetBytes(int64(len(p.data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var m map[string]any
				if err := json.Unmarshal(p.data, &m); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
