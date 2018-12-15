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

func TestStackElExec(t *testing.T) {
	var nilStackEl *stackEl
	savepointer := mock.NewSavepointer(ioutil.Discard, true)

	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	defer db.Close() // nolint:errcheck

	_sqlmock.ExpectBegin()
	_sqlmock.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 0))

	querier, err := NewQuerier(context.Background(), db, savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	el, ok := querier.(*stackEl)
	if !ok {
		t.Fatal("Default querier is not a *stackEl")
	}

	testCases := []struct {
		name        string
		el          *stackEl
		expectedErr error
	}{
		{name: "nil stackEl", el: nilStackEl, expectedErr: ErrNilQuerier},
		{name: "nil tx", el: &stackEl{tx: nil, savepointer: savepointer}, expectedErr: ErrInvalidQuerier},
		{name: "success", el: el, expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.el.Exec(""); err != tc.expectedErr {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}
		})
	}
}

func TestStackElQuery(t *testing.T) {
	var nilStackEl *stackEl
	savepointer := mock.NewSavepointer(ioutil.Discard, true)

	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	defer db.Close() // nolint:errcheck

	_sqlmock.ExpectBegin()
	_sqlmock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))

	querier, err := NewQuerier(context.Background(), db, savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	el, ok := querier.(*stackEl)
	if !ok {
		t.Fatal("Default querier is not a *stackEl")
	}

	testCases := []struct {
		name        string
		el          *stackEl
		expectedErr error
	}{
		{name: "nil stackEl", el: nilStackEl, expectedErr: ErrNilQuerier},
		{name: "nil tx", el: &stackEl{tx: nil, savepointer: savepointer}, expectedErr: ErrInvalidQuerier},
		{name: "success", el: el, expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.el.Query(""); err != tc.expectedErr {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}
		})
	}
}

func TestStackElQueryRow(t *testing.T) {
	var nilStackEl *stackEl
	savepointer := mock.NewSavepointer(ioutil.Discard, true)

	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	defer db.Close() // nolint:errcheck

	_sqlmock.ExpectBegin()
	_sqlmock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))

	querier, err := NewQuerier(context.Background(), db, savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	el, ok := querier.(*stackEl)
	if !ok {
		t.Fatal("Default querier is not a *stackEl")
	}

	testCases := []struct {
		name         string
		el           *stackEl
		expectNilRow bool
	}{
		{name: "nil stackEl", el: nilStackEl, expectNilRow: true},
		{name: "nil tx", el: &stackEl{tx: nil, savepointer: savepointer}, expectNilRow: true},
		{name: "success", el: el, expectNilRow: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if row := tc.el.QueryRow(""); row != nil && tc.expectNilRow {
				t.Errorf("Expected a nil row but got: %+v", row)
			} else if row == nil && !tc.expectNilRow {
				t.Error("Got an unxpected nil row")
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
	tx := sql.Tx{}

	testCases := []struct {
		name        string
		el          *stackEl
		f           func(context.Context, Querier) error
		expectedErr *Error
	}{
		{name: "nil stackEl", el: nilStackEl, f: cb, expectedErr: newError(nil, ErrNilQuerier)},
		{name: "nil tx", el: &stackEl{tx: nil, savepointer: savepointer}, f: cb,
			expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil savepointer", el: &stackEl{tx: &tx, savepointer: nil}, f: cb,
			expectedErr: newError(nil, ErrInvalidQuerier)},
		{name: "nil callback", el: &stackEl{tx: &tx, savepointer: savepointer}, f: nil, expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.el.Atomic(tc.f); !errsEq(err, tc.expectedErr) {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}
		})
	}
}
