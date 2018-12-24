// Package satomic provides a easy way to nest atomic SQL updates using transactions and savepoints
package satomic

import (
	"context"
	"database/sql"
	"errors"
)

import (
	"github.com/dhui/satomic/savepointers"
)

var (
	// ErrNeedsDb is the canonical error value when an attempt to create a Querier doesn't specify a DB
	ErrNeedsDb = errors.New("Need DB to create Querier")
	// ErrNeedsSavepointer is the canonical error value when an attempt to create a Querier doesn't specify a
	// Savepointer
	ErrNeedsSavepointer = errors.New("Need Savepointer to create Querier")
	// ErrNilQuerier is the canonical error value for when a nil Querier is used
	ErrNilQuerier = errors.New("nil Querier")
	// ErrInvalidQuerier is the canonical error value for when an invalid Querier is used
	ErrInvalidQuerier = errors.New("Invalid Querier")
)

// QuerierBase provides an interface containing database/sql methods shared between
// sql.DB, sql.Tx, and sql.Conn
type QuerierBase interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	// TODO:
	// Should support for sql.Conn be dropped?
	// Should support for Prepare(), PrepareContext(), Stmt(), and StmtContext() be added?
}

// Querier provides an interface to interact with a SQL DB within an atomic transaction or savepoint
type Querier interface {
	QuerierBase

	// Atomic runs any SQL statement(s) with the given querier atomically by wrapping the statement(s)
	// in a transaction or savepoint.
	// Any error returned by the callback function (or panic) will result in the rollback of the transaction
	// or rollback to the previous savepoint as appropriate.
	// Otherwise, the previous savepoint will be released or the transaction will be committed.
	//
	// Note: Atomic() is not safe for concurrent use by multiple goroutines. e.g. your SQL statements may be
	// interleaved and thus nonsensical.
	Atomic(f func(context.Context, Querier) error) *Error

	// Context gets the Context associated w/ the Querier
	Context() context.Context
	// Copy makes a shallow copy of the Querier
	Copy() Querier
	// DB gets the DB associated w/ the Querier. A valid Querier should never return nil.
	DB() *sql.DB
	// NewTx creates a new sql.Tx
	NewTx(context.Context, *sql.DB, sql.TxOptions) (*sql.Tx, error)
	// TxOpts gets the sql.TxOptions to use with the Querier's TxCreator
	TxOpts() sql.TxOptions
	// Tx gets the sql.Tx associated w/ the Querier. A valid Querier may return nil.
	Tx() *sql.Tx
	// SetTx sets the sql.Tx associated w/ the Querier
	SetTx(*sql.Tx)
	// Savepointer gets the Savepointer associated w/ the Querier
	Savepointer() savepointers.Savepointer
	// UsingSavepoint returns true if the Querier is using a savepoint and false otherwise
	UsingSavepoint() bool
	// SavepointName gets the name of the savepoint associated w/ the Querier. An empty string
	// will be returned if it's not using a savepoint.
	SavepointName() string
	// SetSavepointName sets the Querier's savepoint name
	SetSavepointName(string)
}

type querier struct {
	ctx           context.Context
	db            *sql.DB
	txOpts        sql.TxOptions
	tx            *sql.Tx
	savepointer   savepointers.Savepointer
	savepointName string
}

func (q *querier) Exec(query string, args ...interface{}) (sql.Result, error) {
	return q.ExecContext(context.Background(), query, args...)
}

func (q *querier) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if q == nil {
		return nil, ErrNilQuerier
	}
	if q.db == nil {
		return nil, ErrInvalidQuerier
	}
	if q.tx == nil {
		return q.db.ExecContext(ctx, query, args...)
	}
	return q.tx.ExecContext(ctx, query, args...)
}

func (q *querier) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return q.QueryContext(context.Background(), query, args...)
}

func (q *querier) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if q == nil {
		return nil, ErrNilQuerier
	}
	if q.db == nil {
		return nil, ErrInvalidQuerier
	}
	if q.tx == nil {
		return q.db.QueryContext(ctx, query, args...)
	}
	return q.tx.QueryContext(ctx, query, args...)
}
func (q *querier) QueryRow(query string, args ...interface{}) *sql.Row {
	return q.QueryRowContext(context.Background(), query, args...)
}

func (q *querier) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if q == nil {
		return nil
	}
	if q.db == nil {
		return nil
	}
	if q.tx == nil {
		return q.db.QueryRowContext(ctx, query, args...)
	}
	return q.tx.QueryRowContext(ctx, query, args...)
}

// using named returns so the deferred function call can modify the returned error
func (q *querier) Atomic(f func(context.Context, Querier) error) (err *Error) {
	return Atomic(q, f)
}

// Context gets the querier's context.Context
func (q *querier) Context() context.Context {
	if q == nil {
		return nil
	}
	return q.ctx
}

// Copy returns a shallow copy of the querier
func (q *querier) Copy() Querier {
	if q == nil {
		return nil
	}
	cpy := *q
	return &cpy
}

// DB gets the querier's db
func (q *querier) DB() *sql.DB {
	if q == nil {
		return nil
	}
	return q.db
}

// NewTx creates a new sql.Tx
func (q *querier) NewTx(ctx context.Context, db *sql.DB, txOpts sql.TxOptions) (*sql.Tx, error) {
	if q == nil {
		return nil, ErrNilQuerier
	}
	if db == nil {
		return nil, ErrNeedsDb
	}
	return db.BeginTx(ctx, &txOpts)
}

// TxOpts gets the querier's transaction options
func (q *querier) TxOpts() sql.TxOptions {
	if q == nil {
		return sql.TxOptions{}
	}
	return q.txOpts
}

// Tx gets the querier'stx
func (q *querier) Tx() *sql.Tx {
	if q == nil {
		return nil
	}
	return q.tx
}

// SetTx sets the querier's tx
func (q *querier) SetTx(tx *sql.Tx) {
	if q == nil {
		return
	}
	q.tx = tx
}

// Savepointer gets the querier's savepointer
func (q *querier) Savepointer() savepointers.Savepointer {
	if q == nil {
		return nil
	}
	return q.savepointer
}

// UsingSavepoint determines whether or not the querier is using a savepoint or transaction
func (q *querier) UsingSavepoint() bool {
	if q == nil {
		return false
	}
	return q.savepointName != ""
}

// SavepointName gets the querier's savepoint name
func (q *querier) SavepointName() string {
	if q == nil {
		return ""
	}
	return q.savepointName
}

// SetSavepointName sets the savepoint name for the querier
func (q *querier) SetSavepointName(name string) {
	if q == nil {
		return
	}
	q.savepointName = name
}

// NewQuerier creates a new Querier
func NewQuerier(ctx context.Context, db *sql.DB, savepointer savepointers.Savepointer,
	txOpts sql.TxOptions) (Querier, error) {
	if db == nil {
		return nil, ErrNeedsDb
	}
	if savepointer == nil {
		return nil, ErrNeedsSavepointer
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	return &querier{ctx: ctx, db: db, txOpts: txOpts, tx: nil, savepointer: savepointer, savepointName: ""}, nil
}
