package sqlx

import (
	"context"
	"database/sql"
	"errors"
)

import (
	"github.com/jmoiron/sqlx"
)

import (
	"github.com/dhui/satomic"
	"github.com/dhui/satomic/savepointers"
)

var (
	// ErrDuplicateTransaction is the canonical error value when a Querier already has a transaction but
	// another transaction is being created
	ErrDuplicateTransaction = errors.New("Querier already has a transaction")
	// ErrDbMismatch is the canonical error value when a Querier's DB doesn't match the DB for creating a transaction
	ErrDbMismatch = errors.New("Querier DB doesn't match DB for transaction creation")
)

// Querier provides an interface to interact with a SQL DB within an atomic transaction or savepoint
type Querier interface {
	satomic.Querier

	// sqlx methods (common between sqlx.DB and sqlx.Tx)
	Get(interface{}, string, ...interface{}) error
	GetContext(context.Context, interface{}, string, ...interface{}) error
	Select(interface{}, string, ...interface{}) error
	SelectContext(context.Context, interface{}, string, ...interface{}) error
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
}

type wrappedQuerier struct {
	satomic.Querier
	db *sqlx.DB
	tx *sqlx.Tx
}

func (wq *wrappedQuerier) Get(dest interface{}, query string, args ...interface{}) error {
	return wq.GetContext(context.Background(), dest, query, args...)
}

func (wq *wrappedQuerier) GetContext(ctx context.Context, dest interface{}, query string,
	args ...interface{}) error {
	if wq == nil {
		return satomic.ErrNilQuerier
	}
	if wq.db == nil {
		return satomic.ErrInvalidQuerier
	}
	if wq.tx == nil {
		return wq.db.GetContext(ctx, dest, query, args...)
	}
	return wq.tx.GetContext(ctx, dest, query, args...)
}

func (wq *wrappedQuerier) Select(dest interface{}, query string, args ...interface{}) error {
	return wq.SelectContext(context.Background(), dest, query, args...)
}

func (wq *wrappedQuerier) SelectContext(ctx context.Context, dest interface{}, query string,
	args ...interface{}) error {
	if wq == nil {
		return satomic.ErrNilQuerier
	}
	if wq.db == nil {
		return satomic.ErrInvalidQuerier
	}
	if wq.tx == nil {
		return wq.db.SelectContext(ctx, dest, query, args...)
	}
	return wq.tx.SelectContext(ctx, dest, query, args...)
}

func (wq *wrappedQuerier) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return wq.QueryxContext(context.Background(), query, args...)
}

func (wq *wrappedQuerier) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows,
	error) {
	if wq == nil {
		return nil, satomic.ErrNilQuerier
	}
	if wq.db == nil {
		return nil, satomic.ErrInvalidQuerier
	}
	if wq.tx == nil {
		return wq.db.QueryxContext(ctx, query, args...)
	}
	return wq.tx.QueryxContext(ctx, query, args...)
}

func (wq *wrappedQuerier) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	return wq.QueryRowxContext(context.Background(), query, args...)
}

func (wq *wrappedQuerier) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	if wq == nil {
		return nil
	}
	if wq.db == nil {
		return nil
	}
	if wq.tx == nil {
		return wq.db.QueryRowxContext(ctx, query, args...)
	}
	return wq.tx.QueryRowxContext(ctx, query, args...)
}

func (wq *wrappedQuerier) txCreator(ctx context.Context, db *sql.DB, txOpts sql.TxOptions) (*sql.Tx, error) {
	if wq == nil {
		return nil, satomic.ErrNilQuerier
	}
	if wq.db == nil {
		return nil, satomic.ErrInvalidQuerier
	}
	if wq.tx != nil {
		return nil, ErrDuplicateTransaction
	}

	if wq.db.DB != db {
		return nil, ErrDbMismatch
	}

	tx, err := wq.db.BeginTxx(ctx, &txOpts)
	if err != nil {
		return nil, err
	}
	wq.tx = tx
	return tx.Tx, nil
}

// NewQuerier creates a new Querier
func NewQuerier(ctx context.Context, db *sqlx.DB, savepointer savepointers.Savepointer,
	txOpts sql.TxOptions) (Querier, error) {
	if db == nil {
		return nil, satomic.ErrNeedsDb
	}

	wq := &wrappedQuerier{db: db}
	q, err := satomic.NewQuerierWithTxCreator(ctx, db.DB, savepointer, txOpts, wq.txCreator)
	if err != nil {
		return nil, err
	}
	wq.Querier = q

	return wq, nil
}
