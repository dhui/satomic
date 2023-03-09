// Package postgres implements a Savepointer for postgres
package postgres

import (
	"strings"
)

// Quote quotes the given Postgres identifier
//
// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
//
// The implmentation is from pq.QuoteIdentifer(). It's copied to avoid a dependency on pq.
func Quote(name string) string {
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}

// Savepointer implements the savepointers.Savepointer interface for Postgres
type Savepointer struct{}

// Create creates a new savepoint with the given name
//
// https://www.postgresql.org/docs/current/sql-savepoint.html
func (sp Savepointer) Create(name string) string {
	return "SAVEPOINT " + Quote(name) + ";"
}

// Rollback rollsback the named savepoint
//
// https://www.postgresql.org/docs/current/sql-rollback-to.html
func (sp Savepointer) Rollback(name string) string {
	return "ROLLBACK TO " + Quote(name) + ";"
}

// Release releases the named savepoint
//
// https://www.postgresql.org/docs/current/sql-release-savepoint.html
func (sp Savepointer) Release(name string) string {
	return "RELEASE " + Quote(name) + ";"
}
