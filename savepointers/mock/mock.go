package mock

import (
	"io"
	"strconv"
	"sync"
)

// UnknownSavepointName is the name used mapping savepoint names fails
const UnknownSavepointName = "UNKNOWN_SAVEPOINT_NAME"

// Savepointer implements the savepointers.Savepointer interface for usage with tests.
type Savepointer struct {
	sync.Mutex
	io.Writer
	num        int
	nameMapper map[string]string
	release    bool
}

// Create creates a new savepoint with the given name
func (sp *Savepointer) Create(name string) string {
	sp.Lock()
	defer sp.Unlock()

	sp.num++
	mappedName := strconv.Itoa(sp.num)
	sp.nameMapper[name] = mappedName

	stmt := "SAVEPOINT " + mappedName + ";"
	sp.Write([]byte(stmt + "\n")) // nolint:errcheck
	return stmt
}

// Rollback rollsback the named savepoint
func (sp *Savepointer) Rollback(name string) string {
	sp.Lock()
	defer sp.Unlock()

	mappedName := sp.nameMapper[name]
	if mappedName == "" {
		mappedName = UnknownSavepointName
	}

	stmt := "ROLLBACK TO " + mappedName + ";"
	sp.Write([]byte(stmt + "\n")) // nolint:errcheck
	return stmt
}

// Release releases the named savepoint
func (sp *Savepointer) Release(name string) string {
	sp.Lock()
	defer sp.Unlock()

	if !sp.release {
		return ""
	}

	mappedName := sp.nameMapper[name]
	if mappedName == "" {
		mappedName = UnknownSavepointName
	}

	stmt := "RELEASE " + mappedName + ";"
	sp.Write([]byte(stmt + "\n")) // nolint:errcheck
	return stmt
}

// NewSavepointer creates a new Savepointer
func NewSavepointer(writer io.Writer, release bool) *Savepointer {
	return &Savepointer{Writer: writer, nameMapper: map[string]string{}, release: release}
}
