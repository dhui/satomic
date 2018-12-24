package satomic

import (
	"context"
)

import (
	"github.com/dhui/satomic/savepointers"
)

// Atomic will use the given Querier and run the given function atomically by wrapping any SQL statement(s) passed to the function's Querier using a transaction or savepoint.
// In most cases, this method should not be used directly. Instead consumers should used Querier.Atomic. This method is provided to extend Querier interfaces
func Atomic(q Querier, f func(context.Context, Querier) error) (err *Error) {
	if q == nil {
		return newError(nil, ErrNilQuerier)
	}
	if q.DB() == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if q.Savepointer() == nil {
		return newError(nil, ErrInvalidQuerier)
	}
	if f == nil {
		return nil
	}

	nextQ := q.Copy()
	if nextQ.Tx() == nil {
		tx, txErr := nextQ.NewTx(nextQ.Context(), nextQ.DB(), nextQ.TxOpts())
		if txErr != nil {
			return newError(nil, txErr)
		}
		nextQ.SetTx(tx)
	} else {
		nextQ.SetSavepointName(savepointers.GenSavepointName())
		if _, execErr := nextQ.Tx().ExecContext(nextQ.Context(),
			nextQ.Savepointer().Create(nextQ.SavepointName())); execErr != nil {
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

			if nextQ.UsingSavepoint() {
				// Rollback savepoint on error
				if _, execErr := nextQ.Tx().ExecContext(nextQ.Context(),
					nextQ.Savepointer().Rollback(nextQ.SavepointName())); execErr != nil {
					err.Atomic = execErr
					return
				}
			} else {
				// Rollback transaction on error
				if rbErr := nextQ.Tx().Rollback(); rbErr != nil {
					err.Atomic = rbErr
					return
				}
			}
		} else {
			if nextQ.UsingSavepoint() {
				// Release savepoint on success
				releaseStmt := nextQ.Savepointer().Release(nextQ.SavepointName())
				if releaseStmt == "" {
					// Some SQL RDBMSs don't support releasing savepoints
					return
				}
				if _, execErr := nextQ.Tx().ExecContext(nextQ.Context(), releaseStmt); execErr != nil {
					err = newError(nil, execErr)
					return
				}
			} else {
				// Commit transaction on success
				if commitErr := nextQ.Tx().Commit(); commitErr != nil {
					err = newError(nil, commitErr)
					return
				}
			}
		}
	}()

	cbErr := f(nextQ.Context(), nextQ)
	if cbErr != nil {
		err = newError(cbErr, nil)
	}

	return // nolint:nakedret
}
