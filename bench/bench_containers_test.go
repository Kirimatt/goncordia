package bench_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
	mongoopts "go.mongodb.org/mongo-driver/mongo/options"

	"github.com/kirimatt/goncordia/driver"
	mongodriver "github.com/kirimatt/goncordia/driver/mongodb"
	pgxv5driver "github.com/kirimatt/goncordia/driver/pgxv5"
	redisdriver "github.com/kirimatt/goncordia/driver/redis"
)

var (
	benchPgxDriver   driver.Driver[pgx.Tx]
	benchMongoDriver driver.Driver[mongo.SessionContext]
	benchRedisDriver driver.Driver[redisdriver.NoTx]
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var cleanups []func()

	if d, cleanup := startPostgres(ctx); d != nil {
		benchPgxDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startMongo(ctx); d != nil {
		benchMongoDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startRedis(ctx); d != nil {
		benchRedisDriver = d
		cleanups = append(cleanups, cleanup)
	}

	code := m.Run()

	for _, c := range cleanups {
		c()
	}
	os.Exit(code)
}

func startPostgres(ctx context.Context) (driver.Driver[pgx.Tx], func()) {
	ctr, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase("bench"),
		tcpostgres.WithUsername("bench"),
		tcpostgres.WithPassword("bench"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip postgres benchmarks: %v\n", err)
		return nil, nil
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	d := pgxv5driver.New(pool)
	if err := d.Migrate(ctx); err != nil {
		pool.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	return d, func() {
		pool.Close()
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

func startMongo(ctx context.Context) (driver.Driver[mongo.SessionContext], func()) {
	ctr, err := tcmongo.Run(ctx, "mongo:8.0", tcmongo.WithReplicaSet("rs0"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip mongodb benchmarks: %v\n", err)
		return nil, nil
	}
	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	if strings.Contains(uri, "?") {
		uri += "&directConnection=true"
	} else {
		uri += "?directConnection=true"
	}
	client, err := mongo.Connect(ctx, mongoopts.Client().ApplyURI(uri))
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	d, err := mongodriver.New(ctx, client, "bench")
	if err != nil {
		client.Disconnect(ctx) //nolint:errcheck
		ctr.Terminate(ctx)     //nolint:errcheck
		return nil, nil
	}
	if err := d.Migrate(ctx); err != nil {
		client.Disconnect(ctx) //nolint:errcheck
		ctr.Terminate(ctx)     //nolint:errcheck
		return nil, nil
	}
	return d, func() {
		client.Disconnect(ctx) //nolint:errcheck
		ctr.Terminate(ctx)     //nolint:errcheck
	}
}

func startRedis(ctx context.Context) (driver.Driver[redisdriver.NoTx], func()) {
	ctr, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip redis benchmarks: %v\n", err)
		return nil, nil
	}
	addr, err := ctr.Endpoint(ctx, "")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	d := redisdriver.New(rdb)
	if err := d.Migrate(ctx); err != nil {
		rdb.Close()        //nolint:errcheck
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	return d, func() {
		rdb.Close()        //nolint:errcheck
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

// ---- Enqueue ----

func BenchmarkEnqueue_Postgres(b *testing.B) {
	if benchPgxDriver == nil {
		b.Skip("postgres not available")
	}
	benchmarkEnqueue(b, benchPgxDriver)
}

func BenchmarkEnqueue_MongoDB(b *testing.B) {
	if benchMongoDriver == nil {
		b.Skip("mongodb not available")
	}
	benchmarkEnqueue(b, benchMongoDriver)
}

func BenchmarkEnqueue_Redis(b *testing.B) {
	if benchRedisDriver == nil {
		b.Skip("redis not available")
	}
	benchmarkEnqueue(b, benchRedisDriver)
}

// ---- EnqueueBatch(100) ----

func BenchmarkEnqueueBatch100_Postgres(b *testing.B) {
	if benchPgxDriver == nil {
		b.Skip("postgres not available")
	}
	benchmarkEnqueueBatch(b, benchPgxDriver, 100)
}

func BenchmarkEnqueueBatch100_MongoDB(b *testing.B) {
	if benchMongoDriver == nil {
		b.Skip("mongodb not available")
	}
	benchmarkEnqueueBatch(b, benchMongoDriver, 100)
}

func BenchmarkEnqueueBatch100_Redis(b *testing.B) {
	if benchRedisDriver == nil {
		b.Skip("redis not available")
	}
	benchmarkEnqueueBatch(b, benchRedisDriver, 100)
}

// ---- FetchAndComplete ----

func BenchmarkFetchAndComplete_Postgres(b *testing.B) {
	if benchPgxDriver == nil {
		b.Skip("postgres not available")
	}
	benchmarkFetchAndComplete(b, benchPgxDriver)
}

func BenchmarkFetchAndComplete_MongoDB(b *testing.B) {
	if benchMongoDriver == nil {
		b.Skip("mongodb not available")
	}
	benchmarkFetchAndComplete(b, benchMongoDriver)
}

func BenchmarkFetchAndComplete_Redis(b *testing.B) {
	if benchRedisDriver == nil {
		b.Skip("redis not available")
	}
	benchmarkFetchAndComplete(b, benchRedisDriver)
}

// ---- End-to-end ----

func BenchmarkEndToEnd_Postgres_c4(b *testing.B) {
	if benchPgxDriver == nil {
		b.Skip("postgres not available")
	}
	benchmarkEndToEnd(b, benchPgxDriver, 4)
}

func BenchmarkEndToEnd_MongoDB_c4(b *testing.B) {
	if benchMongoDriver == nil {
		b.Skip("mongodb not available")
	}
	benchmarkEndToEnd(b, benchMongoDriver, 4)
}

func BenchmarkEndToEnd_Redis_c4(b *testing.B) {
	if benchRedisDriver == nil {
		b.Skip("redis not available")
	}
	benchmarkEndToEnd(b, benchRedisDriver, 4)
}
