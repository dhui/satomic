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
// sql.DB and sql.Tx
type QuerierBase interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	// TODO:
	// Should support for Prepare(), PrepareContext(), Stmt(), and StmtContext() be added?
}

// Querier provides an interface to interact with a SQL DB within an atomic transaction or savepoint
type Querier interface {
	QuerierBase

	// Atomic runs any SQL statement(s) with the given querier atomicly by wrapping the statement(s)
	// in a transaction or savepoint.
	// Any error returned by the callback function (or panic) will result in the rollback of the transaction
	// or rollback to the previous savepoint as appropriate.
	// Otherwise, the previous savepoint will be released or the transaction will be committed.
	//
	// Note: Atomic() is not safe for concurrent use by multiple goroutines. e.g. your SQL statements may be
	// interleaved and thus nonsensical.
	Atomic(f func(context.Context, Querier) error) *Error
}

type querier struct {
	ctx           context.Context
	db            *sql.DB
	txCreator     TxCreator
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
	// q should never be modified, instead a nextQ should be created and used

	if q == nil {
		return newError(nil, ErrNilQuerier)
	}
	if q.db == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if q.txCreator == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if q.savepointer == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if f == nil {
		return nil
	}

	nextQ := *q
	if nextQ.tx == nil {
		tx, txErr := nextQ.txCreator(nextQ.ctx, nextQ.db, nextQ.txOpts)
		if txErr != nil {
			return newError(nil, txErr)
		}
		nextQ.tx = tx
	} else {
		nextQ.savepointName = savepointers.GenSavepointName()
		if _, execErr := nextQ.tx.ExecContext(nextQ.ctx,
			nextQ.savepointer.Create(nextQ.savepointName)); execErr != nil {
			return newError(nil, execErr)
		}
	}

	/***************************************************************
	* After this comment/deferred call, named returns must be used *
	***************************************************************/
	defer func() {
		// TODO: don't do anything if we're dealing with an empty orig error
		if r := recover(); err != nil || r != nil {
			if r != nil {
				// re-throw panic
				defer func() {
					panic(r)
				}()
			}

			if nextQ.usingSavepoint() {
				// Rollback savepoint on error
				if _, execErr := nextQ.tx.ExecContext(nextQ.ctx,
					nextQ.savepointer.Rollback(nextQ.savepointName)); execErr != nil {
					err.Atomic = execErr
					return
				}
			} else {
				// Rollback transaction on error
				if rbErr := nextQ.tx.Rollback(); rbErr != nil {
					err.Atomic = rbErr
					return
				}
			}
		} else {
			if nextQ.usingSavepoint() {
				// Release savepoint on success
				releaseStmt := nextQ.savepointer.Release(nextQ.savepointName)
				if releaseStmt == "" {
					// Some SQL RDBMSs don't support releasing savepoints
					return
				}
				if _, execErr := nextQ.tx.ExecContext(nextQ.ctx, releaseStmt); execErr != nil {
					err = newError(nil, execErr)
					return
				}
			} else {
				// Commit transaction on success
				if commitErr := nextQ.tx.Commit(); commitErr != nil {
					err = newError(nil, commitErr)
					return
				}
			}
		}
	}()

	cbErr := f(nextQ.ctx, &nextQ)
	if cbErr != nil {
		err = newError(cbErr, nil)
	}

	return // nolint:nakedret
}

// usingSavepoint determines whether or not the querier is using a savepoint or transaction
func (q *querier) usingSavepoint() bool { return q.savepointName != "" }

// TxCreator is used to create transactions for a Querier
type TxCreator func(context.Context, *sql.DB, sql.TxOptions) (*sql.Tx, error)

// DefaultTxCreator is the default TxCreator to be used
func DefaultTxCreator(ctx context.Context, db *sql.DB, txOpts sql.TxOptions) (*sql.Tx, error) {
	return db.BeginTx(ctx, &txOpts)
}

// NewQuerier creates a new Querier
func NewQuerier(ctx context.Context, db *sql.DB, savepointer savepointers.Savepointer,
	txOpts sql.TxOptions) (Querier, error) {
	return NewQuerierWithTxCreator(ctx, db, savepointer, txOpts, DefaultTxCreator)
}

// NewQuerierWithTxCreator creates a new Querier, allowing the transaction creation to be customized
func NewQuerierWithTxCreator(ctx context.Context, db *sql.DB, savepointer savepointers.Savepointer,
	txOpts sql.TxOptions, txCreator TxCreator) (Querier, error) {
	if db == nil {
		return nil, ErrNeedsDb
	}
	if savepointer == nil {
		return nil, ErrNeedsSavepointer
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	if txCreator == nil {
		txCreator = DefaultTxCreator
	}
	return &querier{ctx: ctx, db: db, txCreator: txCreator, txOpts: txOpts, tx: nil, savepointer: savepointer,
		savepointName: ""}, nil
}
