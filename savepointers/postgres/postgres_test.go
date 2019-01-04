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
)

const (
	timeout = 3 * time.Minute
)

func readyFunc(c dktest.ContainerInfo) bool {
	connStr := fmt.Sprintf("host=%s port=%s user=postgres dbname=postgres sslmode=disable", c.IP, c.Port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return false
	}
	defer db.Close() // nolint:errcheck
	return db.Ping() == nil
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

	for _, v := range versions {
		v := v
		t.Run(v, func(t *testing.T) {
			t.Parallel()
			dktest.Run(t, v, dktest.Options{PortRequired: true, ReadyFunc: readyFunc, Timeout: timeout},
				func(t *testing.T, c dktest.ContainerInfo) {
					connStr := fmt.Sprintf("host=%s port=%s user=postgres dbname=postgres sslmode=disable",
						c.IP, c.Port)
					db, err := sql.Open("postgres", connStr)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close() // nolint:errcheck
					if err := db.Ping(); err != nil {
						t.Fatal(err)
					}
					tx, err := db.Begin()
					if err != nil {
						t.Fatal("Error starting transaction:", err)
					}

					savepointer := postgres.Savepointer{}
					savepointName1 := `needs to be quoted1 +/'"]` + "`"
					savepointName2 := `needs to be quoted2 +/'"]` + "`"
					if _, err := tx.Exec(savepointer.Create(savepointName1)); err != nil {
						t.Fatal("Error creating savepoint:", err)
					}
					if _, err := tx.Exec(savepointer.Rollback(savepointName1)); err != nil {
						t.Fatal("Error rolling back savepoint:", err)
					}
					if _, err := tx.Exec(savepointer.Create(savepointName2)); err != nil {
						t.Fatal("Error creating savepoint:", err)
					}
					if _, err := tx.Exec(savepointer.Release(savepointName2)); err != nil {
						t.Fatal("Error releasing savepoint:", err)
					}
				})
		})
	}
}
