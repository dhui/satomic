package satomic_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"testing"
)

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

import (
	"github.com/dhui/satomic"
	"github.com/dhui/satomic/satomictest"
	"github.com/dhui/satomic/savepointers"
	"github.com/dhui/satomic/savepointers/mock"
)

func TestDefaultQuerierAtomicNoSavepoint(t *testing.T) {
	beginErr := errors.New("begin error")
	expectedBeginErr := satomictest.NewError(nil, beginErr)

	selectErr := errors.New("select 1 error")
	expectedSelectErr := satomictest.NewError(selectErr, nil)

	rbErr := errors.New("rollback error")
	expectedRbErr := satomictest.NewError(selectErr, rbErr)

	commitErr := errors.New("commit error")
	expectedCommitErr := satomictest.NewError(nil, commitErr)

	testCases := []struct {
		name        string
		mocker      func(sqlmock.Sqlmock) sqlmock.Sqlmock
		expectedErr *satomic.Error
	}{
		{name: "success", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectCommit()
			return m
		}, expectedErr: nil},
		{name: "begin err", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin().WillReturnError(beginErr)
			return m
		}, expectedErr: expectedBeginErr},
		{name: "commit error", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectCommit().WillReturnError(commitErr)
			return m
		}, expectedErr: expectedCommitErr},
		{name: "select error", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnError(selectErr)
			m.ExpectRollback()
			return m
		}, expectedErr: expectedSelectErr},
		{name: "select error, rollback error", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnError(selectErr)
			m.ExpectRollback().WillReturnError(rbErr)
			return m
		}, expectedErr: expectedRbErr},
	}

	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, _sqlmock, err := sqlmock.New()
			if err != nil {
				t.Fatal("Error creating sqlmock:", err)
			}
			defer db.Close() // nolint:errcheck

			_sqlmock = tc.mocker(_sqlmock)

			q, err := satomic.NewQuerier(ctx, db, mock.NewSavepointer(io.Discard, true), sql.TxOptions{})
			if err != nil {
				t.Fatal("Error creating Querier:", err)
			}
			if err := q.Atomic(func(ctx context.Context, q satomic.Querier) error {
				var dummy int
				return q.QueryRowContext(ctx, "SELECT 1;").Scan(&dummy)
			}); !satomictest.ErrsEq(err, tc.expectedErr) {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}

			if err := _sqlmock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestDefaultQuerierAtomicNoSavepointPanic(t *testing.T) {
	rbErr := errors.New("rollback error")
	expectedRbErr := satomictest.NewError(nil, rbErr)

	testCases := []struct {
		name        string
		mocker      func(sqlmock.Sqlmock) sqlmock.Sqlmock
		panicVal    interface{}
		expectedErr *satomic.Error
	}{
		{name: "rollback success", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectRollback()
			return m
		}, panicVal: "whoa!", expectedErr: nil},
		{name: "rollback error", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectRollback().WillReturnError(rbErr)
			return m
		}, panicVal: "whoa!", expectedErr: expectedRbErr},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, _sqlmock, err := sqlmock.New()
			if err != nil {
				t.Fatal("Error creating sqlmock:", err)
			}
			defer db.Close() // nolint:errcheck

			_sqlmock = tc.mocker(_sqlmock)

			ctx := context.Background()
			q, err := satomic.NewQuerier(ctx, db, mock.NewSavepointer(io.Discard, true),
				sql.TxOptions{})
			if err != nil {
				t.Fatal("Error creating Querier:", err)
			}
			defer func() {
				if r := recover(); r != tc.panicVal {
					t.Errorf("Didn't get the expected panic value: %+v != %+v", r, tc.panicVal)
				}
			}()
			if err := q.Atomic(func(ctx context.Context, q satomic.Querier) error {
				var dummy int
				if err := q.QueryRowContext(ctx, "SELECT 1;").Scan(&dummy); err != nil {
					t.Error(err)
				}
				panic(tc.panicVal)
			}); !satomictest.ErrsEq(err, tc.expectedErr) {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}

			if err := _sqlmock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestDefaultQuerierAtomicSingleSavepointReleased(t *testing.T) {
	rbErr := errors.New("rollback error")
	expectedRbErr := satomictest.NewError(nil, rbErr)

	createErr := errors.New("savepoint error")
	expectedCreateErr := satomictest.NewError(nil, createErr)

	selectErr := errors.New("select 2 error")
	expectedSelectErr := satomictest.NewError(selectErr, nil)

	releaseErr := errors.New("release error")
	expectedReleaseErr := satomictest.NewError(selectErr, releaseErr)

	savepointer := func(release bool) *mock.Savepointer {
		return mock.NewSavepointer(io.Discard, release)
	}

	testCases := []struct {
		name        string
		savepointer *mock.Savepointer
		mocker      func(sqlmock.Sqlmock) sqlmock.Sqlmock
		expectedErr *satomic.Error
	}{
		{name: "success", savepointer: savepointer(true), mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectExec("SAVEPOINT 1;").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery("SELECT 2;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(2))
			m.ExpectExec("RELEASE 1;").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectCommit()
			return m
		}, expectedErr: nil},
		{name: "no release", savepointer: savepointer(false), mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectExec("SAVEPOINT 1;").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery("SELECT 2;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(2))
			m.ExpectCommit()
			return m
		}, expectedErr: nil},
		{name: "release error", savepointer: savepointer(true), mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectExec("SAVEPOINT 1;").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery("SELECT 2;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(2))
			m.ExpectExec("RELEASE 1;").WillReturnError(rbErr)
			m.ExpectCommit()
			return m
		}, expectedErr: expectedRbErr},
		{name: "create error", savepointer: savepointer(true), mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectExec("SAVEPOINT 1;").WillReturnError(createErr)
			m.ExpectCommit()
			return m
		}, expectedErr: expectedCreateErr},
		{name: "select error", savepointer: savepointer(true), mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectExec("SAVEPOINT 1;").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery("SELECT 2;").WillReturnError(selectErr)
			m.ExpectExec("ROLLBACK TO 1;").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectCommit()
			return m
		}, expectedErr: expectedSelectErr},
		{name: "rollback error", savepointer: savepointer(true), mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectExec("SAVEPOINT 1;").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery("SELECT 2;").WillReturnError(selectErr)
			m.ExpectExec("ROLLBACK TO 1;").WillReturnError(releaseErr)
			m.ExpectCommit()
			return m
		}, expectedErr: expectedReleaseErr},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			db, _sqlmock, err := sqlmock.New()
			if err != nil {
				t.Fatal("Error creating sqlmock:", err)
			}
			defer db.Close() // nolint:errcheck

			_sqlmock = tc.mocker(_sqlmock)

			ctx := context.Background()
			q, err := satomic.NewQuerier(ctx, db, tc.savepointer, sql.TxOptions{})
			if err != nil {
				t.Fatal("Error creating Querier:", err)
			}
			if err := q.Atomic(func(ctx context.Context, q satomic.Querier) error {
				var dummy int
				if err := q.QueryRowContext(ctx, "SELECT 1;").Scan(&dummy); err != nil {
					t.Log("error in select 1:", err)
					return err
				}
				if err := q.Atomic(func(ctx context.Context, q satomic.Querier) error {
					return q.QueryRowContext(ctx, "SELECT 2;").Scan(&dummy)
				}); !satomictest.ErrsEq(err, tc.expectedErr) {
					t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
				}
				return nil
			}); err != nil {
				t.Error(err)
			}

			if err := _sqlmock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestNewQuerierWithTxCreator(t *testing.T) {
	noopMocker := func(m sqlmock.Sqlmock) sqlmock.Sqlmock { return m }

	pingErr := errors.New("ping error")
	pingErrMocker := func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
		m.ExpectPing().WillReturnError(pingErr)
		return m
	}

	getDb := func() (*sql.DB, sqlmock.Sqlmock) {
		db, _sqlmock, err := sqlmock.New()
		if err != nil {
			t.Fatal("Error creating sqlmock:", err)
		}
		return db, _sqlmock
	}

	testCases := []struct {
		name        string
		mocker      func(sqlmock.Sqlmock) sqlmock.Sqlmock
		getDb       func() (*sql.DB, sqlmock.Sqlmock)
		savepointer savepointers.Savepointer
		txCreator   satomic.TxCreator
		expectedErr error
	}{
		{name: "nil db", mocker: noopMocker,
			getDb: func() (*sql.DB, sqlmock.Sqlmock) {
				_, _sqlmock := getDb()
				return nil, _sqlmock
			}, savepointer: mock.NewSavepointer(io.Discard, true), txCreator: satomic.DefaultTxCreator,
			expectedErr: satomic.ErrNeedsDb},
		{name: "nil savepointer", mocker: noopMocker, getDb: getDb, savepointer: nil,
			txCreator: satomic.DefaultTxCreator, expectedErr: satomic.ErrNeedsSavepointer},
		{name: "success", mocker: noopMocker, getDb: getDb, savepointer: mock.NewSavepointer(io.Discard, true),
			txCreator: satomic.DefaultTxCreator, expectedErr: nil},
		{name: "success - nil TxCreator", mocker: noopMocker, getDb: getDb,
			savepointer: mock.NewSavepointer(io.Discard, true), txCreator: nil, expectedErr: nil},
		{name: "error", mocker: pingErrMocker, getDb: func() (*sql.DB, sqlmock.Sqlmock) {
			db, _sqlmock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			if err != nil {
				t.Fatal("Error creating sqlmock:", err)
			}
			return db, _sqlmock
		}, savepointer: mock.NewSavepointer(io.Discard, true),
			txCreator: satomic.DefaultTxCreator, expectedErr: pingErr},
	}

	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, _sqlmock := tc.getDb()
			_sqlmock = tc.mocker(_sqlmock)

			if _, err := satomic.NewQuerierWithTxCreator(ctx, db, tc.savepointer,
				sql.TxOptions{}, tc.txCreator); err != tc.expectedErr {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}

			if err := _sqlmock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestQuerierBaseImplementers(t *testing.T) { //nolint:revive
	f := func(_ satomic.QuerierBase) {}

	// Test that sql.DB implements the satomic.QuerierBase interface
	f(&sql.DB{})
	// Test that sql.Tx implements the satomic.QuerierBase interface
	f(&sql.Tx{})
}
