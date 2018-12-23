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

type stackEl struct {
	ctx           context.Context
	db            *sql.DB
	txCreator     TxCreator
	txOpts        sql.TxOptions
	tx            *sql.Tx
	savepointer   savepointers.Savepointer
	savepointName string
}

func (el *stackEl) Exec(query string, args ...interface{}) (sql.Result, error) {
	return el.ExecContext(context.Background(), query, args...)
}

func (el *stackEl) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if el == nil {
		return nil, ErrNilQuerier
	}
	if el.db == nil {
		return nil, ErrInvalidQuerier
	}
	if el.tx == nil {
		return el.db.ExecContext(ctx, query, args...)
	}
	return el.tx.ExecContext(ctx, query, args...)
}

func (el *stackEl) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return el.QueryContext(context.Background(), query, args...)
}

func (el *stackEl) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if el == nil {
		return nil, ErrNilQuerier
	}
	if el.db == nil {
		return nil, ErrInvalidQuerier
	}
	if el.tx == nil {
		return el.db.QueryContext(ctx, query, args...)
	}
	return el.tx.QueryContext(ctx, query, args...)
}
func (el *stackEl) QueryRow(query string, args ...interface{}) *sql.Row {
	return el.QueryRowContext(context.Background(), query, args...)
}

func (el *stackEl) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if el == nil {
		return nil
	}
	if el.db == nil {
		return nil
	}
	if el.tx == nil {
		return el.db.QueryRowContext(ctx, query, args...)
	}
	return el.tx.QueryRowContext(ctx, query, args...)
}

// using named returns so the deferred function call can modify the returned error
func (el *stackEl) Atomic(f func(context.Context, Querier) error) (err *Error) {
	// el should never be modified, instead a nextEl should be created and used

	if el == nil {
		return newError(nil, ErrNilQuerier)
	}
	if el.db == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if el.txCreator == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if el.savepointer == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if f == nil {
		return nil
	}

	nextEl := *el
	if nextEl.tx == nil {
		tx, txErr := nextEl.txCreator(nextEl.ctx, nextEl.db, nextEl.txOpts)
		if txErr != nil {
			return newError(nil, txErr)
		}
		nextEl.tx = tx
	} else {
		nextEl.savepointName = savepointers.GenSavepointName()
		if _, execErr := nextEl.tx.ExecContext(nextEl.ctx,
			nextEl.savepointer.Create(nextEl.savepointName)); execErr != nil {
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

			if nextEl.usingSavepoint() {
				// Rollback savepoint on error
				if _, execErr := nextEl.tx.ExecContext(nextEl.ctx,
					nextEl.savepointer.Rollback(nextEl.savepointName)); execErr != nil {
					err.Atomic = execErr
					return
				}
			} else {
				// Rollback transaction on error
				if rbErr := nextEl.tx.Rollback(); rbErr != nil {
					err.Atomic = rbErr
					return
				}
			}
		} else {
			if nextEl.usingSavepoint() {
				// Release savepoint on success
				releaseStmt := nextEl.savepointer.Release(nextEl.savepointName)
				if releaseStmt == "" {
					// Some SQL RDBMSs don't support releasing savepoints
					return
				}
				if _, execErr := nextEl.tx.ExecContext(nextEl.ctx, releaseStmt); execErr != nil {
					err = newError(nil, execErr)
					return
				}
			} else {
				// Commit transaction on success
				if commitErr := nextEl.tx.Commit(); commitErr != nil {
					err = newError(nil, commitErr)
					return
				}
			}
		}
	}()

	cbErr := f(nextEl.ctx, &nextEl)
	if cbErr != nil {
		err = newError(cbErr, nil)
	}

	return // nolint:nakedret
}

// useSavepoint determines whether or not the stack element is using a savepoint or transaction
func (el *stackEl) usingSavepoint() bool { return el.savepointName != "" }

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
	return &stackEl{ctx: ctx, db: db, txCreator: txCreator, txOpts: txOpts, tx: nil, savepointer: savepointer,
		savepointName: ""}, nil
}
