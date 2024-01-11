// Package mssql implements a Savepointer for mssql
package mssql

import (
	"strings"
)

// Quote quotes the given MS SQL identifier
//
// https://docs.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers
func Quote(name string) string {
	return `[` + strings.Replace(name, `]`, `]]`, -1) + `]`
}

// Savepointer implements the savepointers.Savepointer interface for MS SQL
type Savepointer struct{}

// Create creates a new savepoint with the given name
//
// Note: names have a max length of 32 characters
//
// https://docs.microsoft.com/en-us/sql/t-sql/language-elements/save-transaction-transact-sql
func (sp Savepointer) Create(name string) string {
	return "SAVE TRANSACTION " + Quote(name) + ";"
}

// Rollback rollsback the named savepoint
//
// https://docs.microsoft.com/en-us/sql/t-sql/language-elements/rollback-work-transact-sql
func (sp Savepointer) Rollback(name string) string {
	return "ROLLBACK TRANSACTION " + Quote(name) + ";"
}

// Release releases the named savepoint. Releasing a savepoint is not implemented in MS SQL
func (sp Savepointer) Release(name string) string { //nolint:revive
	return ""
}
