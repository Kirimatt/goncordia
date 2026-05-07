package bench_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tccassandra "github.com/testcontainers/testcontainers-go/modules/cassandra"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
	mongoopts "go.mongodb.org/mongo-driver/mongo/options"

	"github.com/kirimatt/goncordia/driver"
	cassandradriver "github.com/kirimatt/goncordia/driver/cassandra"
	clickhousedriver "github.com/kirimatt/goncordia/driver/clickhouse"
	mongodriver "github.com/kirimatt/goncordia/driver/mongodb"
	pgxv5driver "github.com/kirimatt/goncordia/driver/pgxv5"
	redisdriver "github.com/kirimatt/goncordia/driver/redis"
)

var (
	benchPgxDriver        driver.Driver[pgx.Tx]
	benchMongoDriver      driver.Driver[mongo.SessionContext]
	benchRedisDriver      driver.Driver[redisdriver.NoTx]
	benchCassandraDriver  driver.Driver[cassandradriver.NoTx]
	benchClickHouseDriver driver.Driver[clickhousedriver.NoTx]
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
	if d, cleanup := startCassandra(ctx); d != nil {
		benchCassandraDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startClickHouse(ctx); d != nil {
		benchClickHouseDriver = d
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

func startCassandra(ctx context.Context) (driver.Driver[cassandradriver.NoTx], func()) {
	ctr, err := tccassandra.Run(ctx, "cassandra:4.1")
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip cassandra benchmarks: %v\n", err)
		return nil, nil
	}
	host, err := ctr.ConnectionHost(ctx)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	cluster := gocql.NewCluster(host)
	cluster.Timeout = 15 * time.Second
	cluster.ConnectTimeout = 15 * time.Second
	cluster.Consistency = gocql.Quorum

	sysSession, err := cluster.CreateSession()
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	if err := sysSession.Query(
		`CREATE KEYSPACE IF NOT EXISTS goncordia_bench
		 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
	).Exec(); err != nil {
		sysSession.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	sysSession.Close()

	cluster.Keyspace = "goncordia_bench"
	session, err := cluster.CreateSession()
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	d := cassandradriver.New(session)
	if err := d.Migrate(ctx); err != nil {
		session.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	return d, func() {
		session.Close()
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

func startClickHouse(ctx context.Context) (driver.Driver[clickhousedriver.NoTx], func()) {
	ctr, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:24.3-alpine")
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip clickhouse benchmarks: %v\n", err)
		return nil, nil
	}
	dsn, err := ctr.ConnectionString(ctx)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	d := clickhousedriver.New(conn)
	if err := d.Migrate(ctx); err != nil {
		conn.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	return d, func() {
		conn.Close()
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

func BenchmarkEnqueue_Cassandra(b *testing.B) {
	if benchCassandraDriver == nil {
		b.Skip("cassandra not available")
	}
	benchmarkEnqueue(b, benchCassandraDriver)
}

func BenchmarkEnqueue_ClickHouse(b *testing.B) {
	if benchClickHouseDriver == nil {
		b.Skip("clickhouse not available")
	}
	benchmarkEnqueue(b, benchClickHouseDriver)
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

func BenchmarkEnqueueBatch100_Cassandra(b *testing.B) {
	if benchCassandraDriver == nil {
		b.Skip("cassandra not available")
	}
	benchmarkEnqueueBatch(b, benchCassandraDriver, 100)
}

func BenchmarkEnqueueBatch100_ClickHouse(b *testing.B) {
	if benchClickHouseDriver == nil {
		b.Skip("clickhouse not available")
	}
	benchmarkEnqueueBatch(b, benchClickHouseDriver, 100)
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

func BenchmarkFetchAndComplete_Cassandra(b *testing.B) {
	if benchCassandraDriver == nil {
		b.Skip("cassandra not available")
	}
	benchmarkFetchAndComplete(b, benchCassandraDriver)
}

func BenchmarkFetchAndComplete_ClickHouse(b *testing.B) {
	if benchClickHouseDriver == nil {
		b.Skip("clickhouse not available")
	}
	benchmarkFetchAndComplete(b, benchClickHouseDriver)
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

func BenchmarkEndToEnd_Cassandra_c4(b *testing.B) {
	if benchCassandraDriver == nil {
		b.Skip("cassandra not available")
	}
	benchmarkEndToEnd(b, benchCassandraDriver, 4)
}

func BenchmarkEndToEnd_ClickHouse_c4(b *testing.B) {
	if benchClickHouseDriver == nil {
		b.Skip("clickhouse not available")
	}
	benchmarkEndToEnd(b, benchClickHouseDriver, 4)
}
