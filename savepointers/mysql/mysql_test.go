package mysql_test

import (
	"database/sql"
	"fmt"
	"testing"
)

import (
	"github.com/dhui/dktest"
	_ "github.com/go-sql-driver/mysql"
)

import (
	"github.com/dhui/satomic/savepointers/mysql"
)

const (
	password = "insecurepassword"
	db       = "public"
)

var env = map[string]string{
	"MYSQL_ROOT_PASSWORD": password,
	"MYSQL_DATABASE":      db,
}

func readyFunc(c dktest.ContainerInfo) bool {
	connStr := fmt.Sprintf("root:%s@tcp(%s:%s)/%s", password, c.IP, c.Port, db)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return false
	}
	defer db.Close() // nolint:errcheck
	return db.Ping() == nil
}

func TestSavepointerMySQL(t *testing.T) {
	t.Parallel()

	// https://www.mysql.com/support/supportedplatforms/database.html
	versions := []string{
		"mysql:8",
		"mysql:5.7",
		"mysql:5.6",
		"mysql:5.5",
	}

	for _, v := range versions {
		v := v
		t.Run(v, func(t *testing.T) {
			t.Parallel()
			dktest.Run(t, v, dktest.Options{Env: env, PortRequired: true, ReadyFunc: readyFunc},
				func(t *testing.T, c dktest.ContainerInfo) {
					connStr := fmt.Sprintf("root:%s@tcp(%s:%s)/%s", password, c.IP, c.Port, db)
					db, err := sql.Open("mysql", connStr)
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

					savepointer := mysql.Savepointer{}
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
