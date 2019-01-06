package postgres_test

import (
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

var postgresDBGetter savepointertest.DBGetter = func(c dktest.ContainerInfo) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%s user=postgres dbname=postgres sslmode=disable", c.IP, c.Port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func TestSavepointerPostgres(t *testing.T) {
	t.Parallel()

	// https://www.postgresql.org/support/versioning/
	versions := []string{
		"postgres:11-alpine",
		"postgres:10.6-alpine",
		"postgres:9.6-alpine",
		"postgres:9.5-alpine",
		"postgres:9.4-alpine",
	}

	savepointertest.TestSavepointerWithDocker(t, postgres.Savepointer{}, versions, dktest.Options{
		PortRequired: true, ReadyFunc: postgresDBGetter.ReadyFunc(), Timeout: timeout}, postgresDBGetter)
}
