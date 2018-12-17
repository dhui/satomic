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

// Querier provides an interface to interact with a SQL DB within an atomic transaction or savepoint
type Querier interface {
	// database/sql methods (common between sql.DB, sql.Tx, and sql.Conn)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	// TODO: Add Prepare(), PrepareContext(), Stmt(), and StmtContext()

	// Atomic runs any SQL statement(s) with the given querier atomicly by wrapping the statement(s)
	// in a transaction or savepoint.
	//
	// Note: Atomic() is not safe for concurrent use by multiple goroutines. e.g. your SQL statements may be
	// interleaved and thus nonsensical.
	Atomic(f func(context.Context, Querier) error) *Error
}

type stackEl struct {
	ctx           context.Context
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
	if el.tx == nil {
		return nil, ErrInvalidQuerier
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
	if el.tx == nil {
		return nil, ErrInvalidQuerier
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
	if el.tx == nil {
		return nil
	}
	return el.tx.QueryRowContext(ctx, query, args...)
}

func (el *stackEl) Atomic(f func(context.Context, Querier) error) (err *Error) {
	if el == nil {
		return newError(nil, ErrNilQuerier)
	}
	if el.tx == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if el.savepointer == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if f == nil {
		return nil
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
			if err.Err == nil {
				return
			}

			if el.usingSavepoint() {
				// Rollback savepoint on error
				if _, execErr := el.tx.ExecContext(el.ctx,
					el.savepointer.Rollback(el.savepointName)); execErr != nil {
					err.Atomic = execErr
					return
				}
			} else {
				// Rollback transaction on error
				if rbErr := el.tx.Rollback(); rbErr != nil {
					err.Atomic = rbErr
					return
				}
			}
		} else {
			if el.usingSavepoint() {
				// Release savepoint on success
				releaseStmt := el.savepointer.Release(el.savepointName)
				if releaseStmt == "" {
					// Some SQL RDBMSs don't support releasing savepoints
					return
				}
				if _, execErr := el.tx.ExecContext(el.ctx, releaseStmt); execErr != nil {
					err = newError(nil, execErr)
					return
				}
			} else {
				// Commit transaction on success
				if commitErr := el.tx.Commit(); commitErr != nil {
					err = newError(nil, commitErr)
					return
				}
			}
		}
	}()

	if el.usingSavepoint() {
		if _, execErr := el.tx.ExecContext(el.ctx, el.savepointer.Create(el.savepointName)); execErr != nil {
			err = newError(nil, execErr)
			return
		}
	}
	nextEl := &stackEl{ctx: el.ctx, tx: el.tx, savepointer: el.savepointer, savepointName: savepointers.GenSavepointName()}

	cbErr := f(nextEl.ctx, nextEl)
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
	tx, err := txCreator(ctx, db, txOpts)
	if err != nil {
		return nil, err
	}
	return &stackEl{ctx: ctx, tx: tx, savepointer: savepointer}, nil
}
