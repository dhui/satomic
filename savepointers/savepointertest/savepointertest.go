package savepointertest

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

import (
	"github.com/dhui/dktest"
)

import (
	"github.com/dhui/satomic/savepointers"
)

// DBGetter is a function that gets a ready-to-use SQL DB using the given ContainerInfo.
// e.g. Ping() should be called by the DBGetter before returning the *sql.DB.
// The caller is responsible for closing the DB.
type DBGetter func(context.Context, dktest.ContainerInfo) (*sql.DB, error)

// ReadyFunc converts the DBGetter to dktest compatible ReadyFunc
func (g DBGetter) ReadyFunc() func(context.Context, dktest.ContainerInfo) bool {
	return func(ctx context.Context, c dktest.ContainerInfo) bool {
		db, err := g(ctx, c)
		if err != nil {
			return false
		}
		db.Close() // nolint:errcheck
		return true
	}
}

// TestSavepointer tests the given Savepointer using the given ready-to-use *sql.DB
// e.g. Ping() should already have been called on the *sql.DB
// The caller is responsible for closing the *sql.DB
func TestSavepointer(t *testing.T, savepointer savepointers.Savepointer, db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		t.Fatal("Error starting transaction:", err)
	}

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
}

// TestSavepointerWithDocker tests the given Savepointer using the given Docker images and options
func TestSavepointerWithDocker(t *testing.T, savepointer savepointers.Savepointer, imageNames []string,
	opts dktest.Options, dbGetter DBGetter) {
	ctx := context.Background()
	for _, imageName := range imageNames {
		imageName := imageName
		t.Run(fmt.Sprintf("test_savepointer_%T_%s", savepointer, imageName), func(t *testing.T) {
			t.Parallel()
			dktest.Run(t, imageName, opts,
				func(t *testing.T, c dktest.ContainerInfo) {
					db, err := dbGetter(ctx, c)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close() // nolint:errcheck
					TestSavepointer(t, savepointer, db)
				})
		})
	}
}
