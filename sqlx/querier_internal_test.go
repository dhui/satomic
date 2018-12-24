package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"io/ioutil"
	"testing"
)

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

import (
	"github.com/dhui/satomic"
	"github.com/dhui/satomic/savepointers/mock"
)

type testStruct struct{ ID int }

func genTestRows() *sqlmock.Rows { return sqlmock.NewRows([]string{"id"}).AddRow(1) }

func genWrappedQuerier(t *testing.T, mocker func(sqlmock.Sqlmock) sqlmock.Sqlmock) (*wrappedQuerier,
	sqlmock.Sqlmock) {
	savepointer := mock.NewSavepointer(ioutil.Discard, true)
	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	// defer db.Close() // nolint:errcheck

	if mocker != nil {
		_sqlmock = mocker(_sqlmock)
	}

	q, err := NewQuerier(context.Background(), sqlx.NewDb(db, ""), savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	wq, ok := q.(*wrappedQuerier)
	if !ok {
		t.Fatal("Default querier is not a *wrappedQuerier")
	}
	return wq, _sqlmock
}

func genWrappedQueriers(t *testing.T) (wqNilDb *wrappedQuerier, nilDbSqlmock sqlmock.Sqlmock,
	wqNilTx *wrappedQuerier, nilTxSqlmock sqlmock.Sqlmock,
	wqWithTx *wrappedQuerier, withTxSqlmock sqlmock.Sqlmock) {

	ctx := context.Background()
	wqNilDb, nilDbSqlmock = genWrappedQuerier(t, nil)
	wqNilDb.db = nil

	wqNilTx, nilTxSqlmock = genWrappedQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectQuery("").WillReturnRows(genTestRows())
		return m
	})

	wqWithTx, withTxSqlmock = genWrappedQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		m.ExpectQuery("").WillReturnRows(genTestRows())
		return m
	})
	_, err := wqWithTx.NewTx(ctx, wqWithTx.db.DB, sql.TxOptions{})
	if err != nil {
		t.Fatal("Could not start transaction:", err)
	}
	return
}

func TestWrappedQuerierGet(t *testing.T) {
	var nilWrappedQuerier *wrappedQuerier
	wqNilDb, nilDbSqlmock, wqNilTx, nilTxSqlmock, wqWithTx, withTxSqlmock := genWrappedQueriers(t)

	testCases := []struct {
		name        string
		wq          *wrappedQuerier
		expectedErr error
		_sqlmock    sqlmock.Sqlmock
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, expectedErr: satomic.ErrNilQuerier},
		{name: "nil db", wq: wqNilDb, expectedErr: satomic.ErrInvalidQuerier, _sqlmock: nilDbSqlmock},
		{name: "nil tx", wq: wqNilTx, expectedErr: nil, _sqlmock: nilTxSqlmock},
		{name: "with tx", wq: wqWithTx, expectedErr: nil, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var dummy testStruct
			if err := tc.wq.Get(&dummy, ""); err != tc.expectedErr {
				t.Errorf("Didn't get expected error: %+v != %+v", err, tc.expectedErr)
			}

			if tc._sqlmock != nil {
				if err := tc._sqlmock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestWrappedQuerierSelect(t *testing.T) {
	var nilWrappedQuerier *wrappedQuerier
	wqNilDb, nilDbSqlmock, wqNilTx, nilTxSqlmock, wqWithTx, withTxSqlmock := genWrappedQueriers(t)

	testCases := []struct {
		name        string
		wq          *wrappedQuerier
		expectedErr error
		_sqlmock    sqlmock.Sqlmock
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, expectedErr: satomic.ErrNilQuerier},
		{name: "nil db", wq: wqNilDb, expectedErr: satomic.ErrInvalidQuerier, _sqlmock: nilDbSqlmock},
		{name: "nil tx", wq: wqNilTx, expectedErr: nil, _sqlmock: nilTxSqlmock},
		{name: "with tx", wq: wqWithTx, expectedErr: nil, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var dummy []testStruct
			if err := tc.wq.Select(&dummy, ""); err != tc.expectedErr {
				t.Errorf("Didn't get expected error: %+v != %+v", err, tc.expectedErr)
			}

			if tc._sqlmock != nil {
				if err := tc._sqlmock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestWrappedQuerierQueryx(t *testing.T) {
	var nilWrappedQuerier *wrappedQuerier
	wqNilDb, nilDbSqlmock, wqNilTx, nilTxSqlmock, wqWithTx, withTxSqlmock := genWrappedQueriers(t)

	testCases := []struct {
		name        string
		wq          *wrappedQuerier
		expectedErr error
		_sqlmock    sqlmock.Sqlmock
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, expectedErr: satomic.ErrNilQuerier},
		{name: "nil db", wq: wqNilDb, expectedErr: satomic.ErrInvalidQuerier, _sqlmock: nilDbSqlmock},
		{name: "nil tx", wq: wqNilTx, expectedErr: nil, _sqlmock: nilTxSqlmock},
		{name: "with tx", wq: wqWithTx, expectedErr: nil, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.wq.Queryx(""); err != tc.expectedErr {
				t.Errorf("Didn't get expected error: %+v != %+v", err, tc.expectedErr)
			}

			if tc._sqlmock != nil {
				if err := tc._sqlmock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestWrappedQuerierQueryRowx(t *testing.T) {
	var nilWrappedQuerier *wrappedQuerier
	wqNilDb, nilDbSqlmock, wqNilTx, nilTxSqlmock, wqWithTx, withTxSqlmock := genWrappedQueriers(t)

	testCases := []struct {
		name         string
		wq           *wrappedQuerier
		expectNilRow bool
		_sqlmock     sqlmock.Sqlmock
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, expectNilRow: true},
		{name: "nil db", wq: wqNilDb, expectNilRow: true, _sqlmock: nilDbSqlmock},
		{name: "nil tx", wq: wqNilTx, expectNilRow: false, _sqlmock: nilTxSqlmock},
		{name: "with tx", wq: wqWithTx, expectNilRow: false, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if row := tc.wq.QueryRowx(""); row != nil && tc.expectNilRow {
				t.Errorf("Expected a nil row but got: %+v", row)
			} else if row == nil && !tc.expectNilRow {
				t.Error("Got an unxpected nil row")
			}

			if tc._sqlmock != nil {
				if err := tc._sqlmock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestWrappedQuerierTxCreator(t *testing.T) {
	ctx := context.Background()
	var nilWrappedQuerier *wrappedQuerier

	wqNilDb, nilDbSqlmock := genWrappedQuerier(t, nil)
	wqNilDb.db = nil

	wqNilTx, nilTxSqlmock := genWrappedQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		return m
	})

	wqWithTx, withTxSqlmock := genWrappedQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		return m
	})
	_, err := wqWithTx.NewTx(ctx, wqWithTx.db.DB, sql.TxOptions{})
	if err != nil {
		t.Fatal("Could not start transaction:", err)
	}

	beginErr := errors.New("begin err")
	wqNilTxBeginErr, nilTxBeginErrSqlmock := genWrappedQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin().WillReturnError(beginErr)
		return m
	})

	testCases := []struct {
		name        string
		wq          *wrappedQuerier
		db          *sql.DB
		expectedErr error
		_sqlmock    sqlmock.Sqlmock
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, db: nil, expectedErr: satomic.ErrNilQuerier},
		{name: "nil db", wq: wqNilDb, db: nil, expectedErr: satomic.ErrInvalidQuerier,
			_sqlmock: nilDbSqlmock},
		{name: "existing tx", wq: wqWithTx, db: wqWithTx.db.DB, expectedErr: ErrDuplicateTransaction,
			_sqlmock: withTxSqlmock},
		{name: "db mismatch", wq: wqNilTx, db: wqWithTx.db.DB, expectedErr: ErrDbMismatch, _sqlmock: nil},
		{name: "nil tx", wq: wqNilTx, db: wqNilTx.db.DB, expectedErr: nil, _sqlmock: nilTxSqlmock},
		{name: "nil tx - begin error", wq: wqNilTxBeginErr, db: wqNilTxBeginErr.db.DB, expectedErr: beginErr,
			_sqlmock: nilTxBeginErrSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.wq.NewTx(ctx, tc.db, sql.TxOptions{}); err != tc.expectedErr {
				t.Errorf("Didn't get expected error: %+v != %+v", err, tc.expectedErr)
			}

			if tc._sqlmock != nil {
				if err := tc._sqlmock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}
