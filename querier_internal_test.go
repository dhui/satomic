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

func genStackEl(t *testing.T, mocker func(sqlmock.Sqlmock) sqlmock.Sqlmock) (*stackEl, sqlmock.Sqlmock) {
	savepointer := mock.NewSavepointer(ioutil.Discard, true)
	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	// defer db.Close() // nolint:errcheck

	if mocker != nil {
		_sqlmock = mocker(_sqlmock)
	}

	querier, err := NewQuerier(context.Background(), db, savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	el, ok := querier.(*stackEl)
	if !ok {
		t.Fatal("Default querier is not a *stackEl")
	}
	return el, _sqlmock
}

func TestStackElExec(t *testing.T) {
	ctx := context.Background()
	var nilStackEl *stackEl

	stackElNilDb, nilDbSqlmock := genStackEl(t, nil)
	stackElNilDb.db = nil

	stackElNilTx, nilTxSqlmock := genStackEl(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 0))
		return m
	})

	stackElWithTx, withTxSqlmock := genStackEl(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		m.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 0))
		return m
	})
	tx, err := stackElWithTx.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal("Could not start transaction:", err)
	}
	stackElWithTx.tx = tx

	testCases := []struct {
		name        string
		el          *stackEl
		expectedErr error
		_sqlmock    sqlmock.Sqlmock
	}{
		{name: "nil stackEl", el: nilStackEl, expectedErr: ErrNilQuerier},
		{name: "nil db", el: stackElNilDb, expectedErr: ErrInvalidQuerier, _sqlmock: nilDbSqlmock},
		{name: "nil tx", el: stackElNilTx, expectedErr: nil, _sqlmock: nilTxSqlmock},
		{name: "with tx", el: stackElWithTx, expectedErr: nil, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.el.Exec(""); err != tc.expectedErr {
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

func TestStackElQuery(t *testing.T) {
	ctx := context.Background()
	var nilStackEl *stackEl

	stackElNilDb, nilDbSqlmock := genStackEl(t, nil)
	stackElNilDb.db = nil

	stackElNilTx, nilTxSqlmock := genStackEl(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
		return m
	})

	stackElWithTx, withTxSqlmock := genStackEl(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		m.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
		return m
	})
	tx, err := stackElWithTx.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal("Could not start transaction:", err)
	}
	stackElWithTx.tx = tx

	testCases := []struct {
		name        string
		el          *stackEl
		expectedErr error
		_sqlmock    sqlmock.Sqlmock
	}{
		{name: "nil stackEl", el: nilStackEl, expectedErr: ErrNilQuerier},
		{name: "nil db", el: stackElNilDb, expectedErr: ErrInvalidQuerier, _sqlmock: nilDbSqlmock},
		{name: "nil tx", el: stackElNilTx, expectedErr: nil, _sqlmock: nilTxSqlmock},
		{name: "with tx", el: stackElWithTx, expectedErr: nil, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.el.Query(""); err != tc.expectedErr {
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

func TestStackElQueryRow(t *testing.T) {
	ctx := context.Background()
	var nilStackEl *stackEl

	stackElNilDb, nilDbSqlmock := genStackEl(t, nil)
	stackElNilDb.db = nil

	stackElNilTx, nilTxSqlmock := genStackEl(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
		return m
	})

	stackElWithTx, withTxSqlmock := genStackEl(t, func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectBegin()
		m.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
		return m
	})
	tx, err := stackElWithTx.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatal("Could not start transaction:", err)
	}
	stackElWithTx.tx = tx

	testCases := []struct {
		name         string
		el           *stackEl
		expectNilRow bool
		_sqlmock     sqlmock.Sqlmock
	}{
		{name: "nil stackEl", el: nilStackEl, expectNilRow: true},
		{name: "nil db", el: stackElNilDb, expectNilRow: true, _sqlmock: nilDbSqlmock},
		{name: "nil tx", el: stackElNilTx, expectNilRow: false, _sqlmock: nilTxSqlmock},
		{name: "with tx", el: stackElWithTx, expectNilRow: false, _sqlmock: withTxSqlmock},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if row := tc.el.QueryRow(""); row != nil && tc.expectNilRow {
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

func TestStackElAtomicErrs(t *testing.T) {
	cb := func(context.Context, Querier) error {
		return nil
	}

	var nilStackEl *stackEl
	savepointer := mock.NewSavepointer(ioutil.Discard, true)
	db := sql.DB{}

	testCases := []struct {
		name        string
		el          *stackEl
		f           func(context.Context, Querier) error
		expectedErr *Error
	}{
		{name: "nil stackEl", el: nilStackEl, f: cb, expectedErr: newError(nil, ErrNilQuerier)},
		{name: "nil db", el: &stackEl{db: nil, txCreator: DefaultTxCreator, savepointer: savepointer}, f: cb,
			expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil txCreator", el: &stackEl{db: &db, txCreator: nil, savepointer: nil}, f: cb,
			expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil savepointer", el: &stackEl{db: &db, txCreator: DefaultTxCreator, savepointer: nil}, f: cb,
			expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil callback", el: &stackEl{db: &db, txCreator: DefaultTxCreator, savepointer: savepointer}, f: nil,
			expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.el.Atomic(tc.f); !errsEq(err, tc.expectedErr) {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}
		})
	}
}
