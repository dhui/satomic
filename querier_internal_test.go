package satomic

import (
	"context"
	"database/sql"
	"io/ioutil"
	"testing"
)

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

import (
	"github.com/dhui/satomic/savepointers/mock"
)

// duplicated from atomictest.ErrsEq() to avoid circular imports
func errsEq(a, b *Error) bool {
	if a == b {
		return true
	}
	if a != nil && b != nil {
		return *a == *b
	}
	return false
}

func genQuerier(t *testing.T, mocker func(sqlmock.Sqlmock) sqlmock.Sqlmock) (*querier, sqlmock.Sqlmock) {
	savepointer := mock.NewSavepointer(ioutil.Discard, true)
	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	// defer db.Close() // nolint:errcheck

	if mocker != nil {
		_sqlmock = mocker(_sqlmock)
	}

	_q, err := NewQuerier(context.Background(), db, savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	q, ok := _q.(*querier)
	if !ok {
		t.Fatal("Default Querier is not a *querier")
	}
	return q, _sqlmock
}

func TestQuerierExec(t *testing.T) {
	ctx := context.Background()
	var nilQuerier *querier

	querierNilDb, nilDbSqlmock := genQuerier(t, nil)
	querierNilDb.db = nil

	querierNilTx, nilTxSqlmock := genQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 0))
		return m
	})

	querierWithTx, withTxSqlmock := genQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		m.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 0))
		return m
	})
	tx, err := querierWithTx.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal("Could not start transaction:", err)
	}
	querierWithTx.tx = tx

	testCases := []struct {
		name        string
		q           *querier
		expectedErr error
		_sqlmock    sqlmock.Sqlmock
	}{
		{name: "nil querier", q: nilQuerier, expectedErr: ErrNilQuerier},
		{name: "nil db", q: querierNilDb, expectedErr: ErrInvalidQuerier, _sqlmock: nilDbSqlmock},
		{name: "nil tx", q: querierNilTx, expectedErr: nil, _sqlmock: nilTxSqlmock},
		{name: "with tx", q: querierWithTx, expectedErr: nil, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.q.Exec(""); err != tc.expectedErr {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}

			if tc._sqlmock != nil {
				if err := tc._sqlmock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestQuerierQuery(t *testing.T) {
	ctx := context.Background()
	var nilQuerier *querier

	querierNilDb, nilDbSqlmock := genQuerier(t, nil)
	querierNilDb.db = nil

	querierNilTx, nilTxSqlmock := genQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
		return m
	})

	querierWithTx, withTxSqlmock := genQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		m.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
		return m
	})
	tx, err := querierWithTx.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal("Could not start transaction:", err)
	}
	querierWithTx.tx = tx

	testCases := []struct {
		name        string
		q           *querier
		expectedErr error
		_sqlmock    sqlmock.Sqlmock
	}{
		{name: "nil querier", q: nilQuerier, expectedErr: ErrNilQuerier},
		{name: "nil db", q: querierNilDb, expectedErr: ErrInvalidQuerier, _sqlmock: nilDbSqlmock},
		{name: "nil tx", q: querierNilTx, expectedErr: nil, _sqlmock: nilTxSqlmock},
		{name: "with tx", q: querierWithTx, expectedErr: nil, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.q.Query(""); err != tc.expectedErr {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}

			if tc._sqlmock != nil {
				if err := tc._sqlmock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestQuerierQueryRow(t *testing.T) {
	ctx := context.Background()
	var nilQuerier *querier

	querierNilDb, nilDbSqlmock := genQuerier(t, nil)
	querierNilDb.db = nil

	querierNilTx, nilTxSqlmock := genQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
		return m
	})

	querierWithTx, withTxSqlmock := genQuerier(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		m.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
		return m
	})
	tx, err := querierWithTx.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal("Could not start transaction:", err)
	}
	querierWithTx.tx = tx

	testCases := []struct {
		name         string
		q            *querier
		expectNilRow bool
		_sqlmock     sqlmock.Sqlmock
	}{
		{name: "nil querier", q: nilQuerier, expectNilRow: true},
		{name: "nil db", q: querierNilDb, expectNilRow: true, _sqlmock: nilDbSqlmock},
		{name: "nil tx", q: querierNilTx, expectNilRow: false, _sqlmock: nilTxSqlmock},
		{name: "with tx", q: querierWithTx, expectNilRow: false, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if row := tc.q.QueryRow(""); row != nil && tc.expectNilRow {
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

func TestQuerierAtomicErrs(t *testing.T) {
	cb := func(context.Context, Querier) error {
		return nil
	}

	var nilQuerier *querier
	savepointer := mock.NewSavepointer(ioutil.Discard, true)
	db := sql.DB{}

	testCases := []struct {
		name        string
		q           *querier
		f           func(context.Context, Querier) error
		expectedErr *Error
	}{
		{name: "nil querier", q: nilQuerier, f: cb, expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil db", q: &querier{db: nil, savepointer: savepointer}, f: cb,
			expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil txCreator", q: &querier{db: &db, savepointer: nil}, f: cb,
			expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil savepointer", q: &querier{db: &db, savepointer: nil}, f: cb,
			expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil callback", q: &querier{db: &db, savepointer: savepointer}, f: nil,
			expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.q.Atomic(tc.f); !errsEq(err, tc.expectedErr) {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}
		})
	}
}
