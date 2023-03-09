// Package sqlite implements a Savepointer for sqlite
package sqlite

import (
	"strings"
)

// Quote quotes the given SQLite identifier
//
// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
//
// The implmentation is from pq.QuoteIdentifer(). It's copied to avoid a dependency on pq.
func Quote(name string) string {
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}

// Savepointer implements the savepointers.Savepointer interface for SQLite
type Savepointer struct{}

// Create creates a new savepoint with the given name
//
// https://www.sqlite.org/lang_savepoint.html
func (sp Savepointer) Create(name string) string {
	return "SAVEPOINT " + Quote(name) + ";"
}

// Rollback rollsback the named savepoint
//
// https://www.sqlite.org/lang_savepoint.html
func (sp Savepointer) Rollback(name string) string {
	return "ROLLBACK TO " + Quote(name) + ";"
}

// Release releases the named savepoint
//
// https://www.sqlite.org/lang_savepoint.html
func (sp Savepointer) Release(name string) string {
	return "RELEASE " + Quote(name) + ";"
}
