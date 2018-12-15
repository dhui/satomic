package sqlx

import (
	"context"
	"database/sql"
)

import (
	"github.com/jmoiron/sqlx"
)

import (
	"github.com/dhui/satomic"
	"github.com/dhui/satomic/savepointers"
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
	if wq.tx == nil {
		return satomic.ErrInvalidQuerier
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
	if wq.tx == nil {
		return satomic.ErrInvalidQuerier
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
	if wq.tx == nil {
		return nil, satomic.ErrInvalidQuerier
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
	if wq.tx == nil {
		return nil
	}
	return wq.tx.QueryRowxContext(ctx, query, args...)
}

// NewQuerier creates a new Querier
func NewQuerier(ctx context.Context, db *sqlx.DB, savepointer savepointers.Savepointer,
	txOpts sql.TxOptions) (Querier, error) {
	if db == nil {
		return nil, satomic.ErrNeedsDb
	}

	var tx *sqlx.Tx

	txCreator := func(context.Context, *sql.DB, sql.TxOptions) (*sql.Tx, error) {
		var err error
		tx, err = db.BeginTxx(ctx, &txOpts)
		if err != nil {
			return nil, err
		}
		return tx.Tx, nil
	}

	q, err := satomic.NewQuerierWithTxCreator(ctx, db.DB, savepointer, txOpts, txCreator)
	if err != nil {
		return nil, err
	}

	return &wrappedQuerier{Querier: q, tx: tx}, nil
}
