package mongo

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// testMongoTimeout bounds server selection. The driver's 30s default is paid in
// full on every dial against an unreachable server, which is what pushed this
// package past the 10m test deadline.
const testMongoTimeout = 3 * time.Second

// testMongoURI is the DSN for integration tests. The default carries
// directConnection because the compose Mongo is a single-node replica set
// advertising a hostname the test process cannot resolve.
func testMongoURI() string {
	if v := os.Getenv("EMBER_TEST_MONGO"); v != "" {
		return v
	}
	return "mongodb://localhost:27017/?directConnection=true"
}

// dialTestMongo dials once for the whole package: connectTestMongo runs per
// test method, and an unreachable server costs a full timeout every time.
var dialTestMongo = sync.OnceValues(func() (*mongo.Client, error) {
	client, err := mongo.Connect(options.Client().
		ApplyURI(testMongoURI()).
		SetServerSelectionTimeout(testMongoTimeout))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), testMongoTimeout)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}

	return client, nil
})

// connectTestMongo returns a collection scoped to this test, skipping when no
// Mongo is reachable.
func connectTestMongo(t *testing.T) *mongo.Collection {
	t.Helper()

	if testing.Short() {
		t.Skip("mongo integration test skipped by -short")
	}

	client, err := dialTestMongo()
	if err != nil {
		t.Skipf("mongo unavailable at %s: %v", testMongoURI(), err)
	}

	col := client.Database("ember_test").Collection(fmt.Sprintf("test_%s", t.Name()))
	t.Cleanup(func() { _ = col.Drop(context.Background()) })

	return col
}

// TestMongoReachable fails where the others skip. Every integration test here
// skips when Mongo is missing and the package still prints "ok", so a run that
// exercised nothing looks exactly like a passing one. Use -short to opt out.
func TestMongoReachable(t *testing.T) {
	if testing.Short() {
		t.Skip("mongo integration tests skipped by -short")
	}

	if _, err := dialTestMongo(); err != nil {
		t.Fatalf("no mongo at %s: %v\n"+
			"start one (docker compose up -d mongo), set EMBER_TEST_MONGO, or run with -short",
			testMongoURI(), err)
	}
}
