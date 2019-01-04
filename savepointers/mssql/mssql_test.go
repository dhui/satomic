package mssql_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

import (
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/dhui/dktest"
)

import (
	"github.com/dhui/satomic/savepointers/mssql"
)

const (
	password = "insecurePassword1"
	timeout  = 3 * time.Minute
)

var env = map[string]string{
	// Developer edition (the default) is free
	// License: https://go.microsoft.com/fwlink/?linkid=857698
	"ACCEPT_EULA": "Y",
	"SA_PASSWORD": password,
}

func readyFunc(c dktest.ContainerInfo) bool {
	connStr := fmt.Sprintf("sqlserver://sa:%s@%s:%s", password, c.IP, c.Port)
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return false
	}
	defer db.Close() // nolint:errcheck
	return db.Ping() == nil
}

func TestSavepointerMSSQL(t *testing.T) {
	t.Parallel()

	// https://www.mysql.com/support/supportedplatforms/database.html
	versions := []string{
		"microsoft/mssql-server-linux",
	}

	for _, v := range versions {
		v := v
		t.Run(v, func(t *testing.T) {
			t.Parallel()
			dktest.Run(t, v, dktest.Options{Env: env, PortRequired: true, ReadyFunc: readyFunc, Timeout: timeout},
				func(t *testing.T, c dktest.ContainerInfo) {
					connStr := fmt.Sprintf("sqlserver://sa:%s@%s:%s", password, c.IP, c.Port)
					db, err := sql.Open("sqlserver", connStr)
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

					savepointer := mssql.Savepointer{}
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
