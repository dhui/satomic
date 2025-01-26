package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

import (
	"github.com/dhui/dktest"
	_ "github.com/lib/pq"
)

import (
	"github.com/dhui/satomic/savepointers/postgres"
	"github.com/dhui/satomic/savepointers/savepointertest"
)

const (
	timeout = 3 * time.Minute
)

var postgresDBGetter savepointertest.DBGetter = func(ctx context.Context, c dktest.ContainerInfo) (*sql.DB, error) {
	ip, port, err := c.FirstPort()
	if err != nil {
		return nil, err
	}
	connStr := fmt.Sprintf("host=%s port=%s user=postgres password=postgres dbname=postgres sslmode=disable", ip, port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

func TestSavepointerPostgres(t *testing.T) {
	t.Parallel()

	// https://www.postgresql.org/support/versioning/
	versions := []string{
		"postgres:17-alpine",
		"postgres:16-alpine",
		"postgres:15-alpine",
		"postgres:14-alpine",
		"postgres:13-alpine",
	}

	savepointertest.TestSavepointerWithDocker(t,
		postgres.Savepointer{},
		versions,
		dktest.Options{
			PortRequired: true,
			ReadyFunc:    postgresDBGetter.ReadyFunc(),
			Timeout:      timeout,
			Env:          map[string]string{"POSTGRES_PASSWORD": "postgres"},
		},
		postgresDBGetter)
}
