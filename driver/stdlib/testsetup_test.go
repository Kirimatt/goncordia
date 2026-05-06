package stdlib_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DSNs populated by TestMain; empty string means container failed to start.
var (
	mysqlDSN    string
	postgresDSN string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	code := 0
	defer func() { os.Exit(code) }()

	// --- MySQL ---
	mysqlCtr, err := tcmysql.Run(ctx,
		"mysql:8.0",
		tcmysql.WithDatabase("goncordia_test"),
		tcmysql.WithUsername("goncordia"),
		tcmysql.WithPassword("goncordia"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("port: 3306  MySQL Community Server").
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start mysql container: %v\n", err)
		code = 1
		return
	}
	defer mysqlCtr.Terminate(ctx) //nolint:errcheck

	dsn, err := mysqlCtr.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mysql connection string: %v\n", err)
		code = 1
		return
	}
	mysqlDSN = dsn + "?parseTime=true"

	// --- Postgres ---
	pgCtr, err := tcpostgres.Run(ctx,
		"postgres:17",
		tcpostgres.WithDatabase("goncordia_test"),
		tcpostgres.WithUsername("goncordia"),
		tcpostgres.WithPassword("goncordia"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		code = 1
		return
	}
	defer pgCtr.Terminate(ctx) //nolint:errcheck

	postgresDSN, err = pgCtr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		code = 1
		return
	}

	code = m.Run()
}
