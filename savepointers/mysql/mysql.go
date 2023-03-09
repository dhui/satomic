// Package mysql implements a Savepointer for mysql
package mysql

import (
	"strings"
)

// Quote quotes the given MySQL identifier
//
// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
func Quote(name string) string {
	return "`" + strings.Replace(name, "`", "``", -1) + "`"
}

// Savepointer implements the savepointers.Savepointer interface for MySQL
type Savepointer struct{}

// Create creates a new savepoint with the given name
//
// https://dev.mysql.com/doc/refman/8.0/en/savepoint.html
func (sp Savepointer) Create(name string) string {
	return "SAVEPOINT " + Quote(name) + ";"
}

// Rollback rollsback the named savepoint
//
// https://dev.mysql.com/doc/refman/8.0/en/savepoint.html
func (sp Savepointer) Rollback(name string) string {
	return "ROLLBACK TO " + Quote(name) + ";"
}

// Release releases the named savepoint
//
// https://dev.mysql.com/doc/refman/8.0/en/savepoint.html
func (sp Savepointer) Release(name string) string {
	return "RELEASE SAVEPOINT " + Quote(name) + ";"
}
