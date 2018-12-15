package sqlx

import (
	"context"
	"database/sql"
	"io/ioutil"
	"testing"
)

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

import (
	atomic "github.com/dhui/satomic"
	"github.com/dhui/satomic/savepointers/mock"
)

type testStruct struct{ ID int }

var testRows = sqlmock.NewRows([]string{"id"}).AddRow(1)

func TestWrappedQuerierGet(t *testing.T) {
	var nilWrappedQuerier *wrappedQuerier
	savepointer := mock.NewSavepointer(ioutil.Discard, true)

	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	defer db.Close() // nolint:errcheck

	_sqlmock.ExpectBegin()
	_sqlmock.ExpectQuery("").WillReturnRows(testRows)

	querier, err := NewQuerier(context.Background(), sqlx.NewDb(db, ""), savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	wq, ok := querier.(*wrappedQuerier)
	if !ok {
		t.Fatal("Default querier is not a *wrappedQuerier")
	}

	testCases := []struct {
		name        string
		wq          *wrappedQuerier
		expectedErr error
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, expectedErr: atomic.ErrNilQuerier},
		{name: "nil tx", wq: &wrappedQuerier{Querier: wq.Querier, tx: nil}, expectedErr: atomic.ErrInvalidQuerier},
		{name: "success", wq: wq, expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var dummy testStruct
			if err := tc.wq.Get(&dummy, ""); err != tc.expectedErr {
				t.Errorf("Didn't get expected error: %+v != %+v", err, tc.expectedErr)
			}
		})
	}

}

func TestWrappedQuerierSelect(t *testing.T) {
	var nilWrappedQuerier *wrappedQuerier
	savepointer := mock.NewSavepointer(ioutil.Discard, true)

	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	defer db.Close() // nolint:errcheck

	_sqlmock.ExpectBegin()
	_sqlmock.ExpectQuery("").WillReturnRows(testRows)

	querier, err := NewQuerier(context.Background(), sqlx.NewDb(db, ""), savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	wq, ok := querier.(*wrappedQuerier)
	if !ok {
		t.Fatal("Default querier is not a *wrappedQuerier")
	}

	testCases := []struct {
		name        string
		wq          *wrappedQuerier
		expectedErr error
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, expectedErr: atomic.ErrNilQuerier},
		{name: "nil tx", wq: &wrappedQuerier{Querier: wq.Querier, tx: nil}, expectedErr: atomic.ErrInvalidQuerier},
		{name: "success", wq: wq, expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var dummy []testStruct
			if err := tc.wq.Select(&dummy, ""); err != tc.expectedErr {
				t.Errorf("Didn't get expected error: %+v != %+v", err, tc.expectedErr)
			}
		})
	}

}

func TestWrappedQuerierQueryx(t *testing.T) {
	var nilWrappedQuerier *wrappedQuerier
	savepointer := mock.NewSavepointer(ioutil.Discard, true)

	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	defer db.Close() // nolint:errcheck

	_sqlmock.ExpectBegin()
	_sqlmock.ExpectQuery("").WillReturnRows(testRows)

	querier, err := NewQuerier(context.Background(), sqlx.NewDb(db, ""), savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	wq, ok := querier.(*wrappedQuerier)
	if !ok {
		t.Fatal("Default querier is not a *wrappedQuerier")
	}

	testCases := []struct {
		name        string
		wq          *wrappedQuerier
		expectedErr error
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, expectedErr: atomic.ErrNilQuerier},
		{name: "nil tx", wq: &wrappedQuerier{Querier: wq.Querier, tx: nil}, expectedErr: atomic.ErrInvalidQuerier},
		{name: "success", wq: wq, expectedErr: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.wq.Queryx(""); err != tc.expectedErr {
				t.Errorf("Didn't get expected error: %+v != %+v", err, tc.expectedErr)
			}
		})
	}

}

func TestWrappedQuerierQueryRowx(t *testing.T) {
	var nilWrappedQuerier *wrappedQuerier
	savepointer := mock.NewSavepointer(ioutil.Discard, true)

	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	defer db.Close() // nolint:errcheck

	_sqlmock.ExpectBegin()
	_sqlmock.ExpectQuery("").WillReturnRows(testRows)

	querier, err := NewQuerier(context.Background(), sqlx.NewDb(db, ""), savepointer, sql.TxOptions{})
	if err != nil {
		t.Fatal("Error creating Querier:", err)
	}
	wq, ok := querier.(*wrappedQuerier)
	if !ok {
		t.Fatal("Default querier is not a *wrappedQuerier")
	}

	testCases := []struct {
		name         string
		wq           *wrappedQuerier
		expectNilRow bool
	}{
		{name: "nil wrappedQuerier", wq: nilWrappedQuerier, expectNilRow: true},
		{name: "nil tx", wq: &wrappedQuerier{Querier: wq.Querier, tx: nil}, expectNilRow: true},
		{name: "success", wq: wq, expectNilRow: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if row := tc.wq.QueryRowx(""); row != nil && tc.expectNilRow {
				t.Errorf("Expected a nil row but got: %+v", row)
			} else if row == nil && !tc.expectNilRow {
				t.Error("Got an unxpected nil row")
			}
		})
	}

}
