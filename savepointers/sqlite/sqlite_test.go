package sqlite_test

import (
	"database/sql"
	"testing"
)

import (
	_ "github.com/mattn/go-sqlite3"
)

import (
	"github.com/dhui/satomic/savepointers/savepointertest"
	"github.com/dhui/satomic/savepointers/sqlite"
)

func TestSavepointerSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal("Error opening SQLite db:", err)
	}
	defer db.Close() // nolint:errcheck

	savepointertest.TestSavepointer(t, sqlite.Savepointer{}, db)
}
