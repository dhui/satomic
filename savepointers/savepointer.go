// Package savepointers provides the Savepointer interface
package savepointers

// Savepointer provides an interface for creating SQL statements for managing savepoints
type Savepointer interface {
	// Create returns a SQL statement that creates the named savepoint
	Create(string) string
	// Rollback returns a SQL statement that rollsback to the named savepoint
	Rollback(string) string
	// Release returns a SQL statement that releases the named savepoint. For SQL RDBMS that don't support releasing
	// savepoints, an empty string should be returned.
	Release(string) string
}
