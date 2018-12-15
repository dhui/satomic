package savepointerstests

import (
	"database/sql"
	"testing"
)

import (
	_ "github.com/mattn/go-sqlite3"
)

import (
	"github.com/dhui/satomic/savepointers/sqlite"
)

func TestSavepointerSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal("Error opening SQLite db:", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal("Error starting transaction:", err)
	}

	savepointer := sqlite.Savepointer{}
	savepointName1 := `needs to be quoted1 +/'"]` + "`"
	savepointName2 := `needs to be quoted2 +/'"]` + "`"
	if _, err := tx.Exec(savepointer.Create(savepointName1)); err != nil {
		t.Fatal("Error creating savepoint:", err)
	}
	if _, err := tx.Exec(savepointer.Rollback(savepointName1)); err != nil {
		t.Fatal("Error creating savepoint:", err)
	}
	if _, err := tx.Exec(savepointer.Create(savepointName2)); err != nil {
		t.Fatal("Error creating savepoint:", err)
	}
	if _, err := tx.Exec(savepointer.Release(savepointName2)); err != nil {
		t.Fatal("Error creating savepoint:", err)
	}
}
