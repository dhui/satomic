package atomic_test

import (
	"context"
	"database/sql"
	"errors"
	"io/ioutil"
	"testing"
)

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

import (
	atomic "github.com/dhui/satomic"
	"github.com/dhui/satomic/atomictest"
	"github.com/dhui/satomic/savepointers"
	"github.com/dhui/satomic/savepointers/mock"
)

func TestDefaultQuerierAtomicBeginErr(t *testing.T) {
	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		t.Fatal("Error creating sqlmock:", err)
	}
	defer db.Close() // nolint:errcheck

	expectedErr := errors.New("begin error")
	_sqlmock.ExpectBegin().WillReturnError(expectedErr)

	ctx := context.Background()
	if _, err := atomic.NewQuerier(ctx, db, mock.NewSavepointer(ioutil.Discard, true),
		sql.TxOptions{}); err != expectedErr {
		t.Error("Didn't get the expected error:", err, "!=", expectedErr)
	}
}

func TestDefaultQuerierAtomicNoSavepoint(t *testing.T) {
	selectErr := errors.New("select 1 error")
	expectedSelectErr := atomictest.NewError(selectErr, nil)

	rbErr := errors.New("rollback error")
	expectedRbErr := atomictest.NewError(selectErr, rbErr)

	commitErr := errors.New("commit error")
	expectedCommitErr := atomictest.NewError(nil, commitErr)

	testCases := []struct {
		name        string
		mocker      func(sqlmock.Sqlmock) sqlmock.Sqlmock
		expectedErr *atomic.Error
	}{
		{name: "success", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			m.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{""}).AddRow(1))
			m.ExpectCommit()
			return m
		}, expectedErr: nil},
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

			q, err := atomic.NewQuerier(ctx, db, mock.NewSavepointer(ioutil.Discard, true), sql.TxOptions{})
			if err != nil {
				t.Fatal("Error creating Querier:", err)
			}
			if err := q.Atomic(func(ctx context.Context, q atomic.Querier) error {
				var dummy int
				return q.QueryRowContext(ctx, "SELECT 1;").Scan(&dummy)
			}); !atomictest.ErrsEq(err, tc.expectedErr) {
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
	expectedRbErr := atomictest.NewError(nil, rbErr)

	testCases := []struct {
		name        string
		mocker      func(sqlmock.Sqlmock) sqlmock.Sqlmock
		panicVal    interface{}
		expectedErr *atomic.Error
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
			q, err := atomic.NewQuerier(ctx, db, mock.NewSavepointer(ioutil.Discard, true),
				sql.TxOptions{})
			if err != nil {
				t.Fatal("Error creating Querier:", err)
			}
			defer func() {
				if r := recover(); r != tc.panicVal {
					t.Errorf("Didn't get the expected panic value: %+v != %+v", r, tc.panicVal)
				}
			}()
			if err := q.Atomic(func(ctx context.Context, q atomic.Querier) error {
				var dummy int
				if err := q.QueryRowContext(ctx, "SELECT 1;").Scan(&dummy); err != nil {
					t.Error(err)
				}
				panic(tc.panicVal)
			}); !atomictest.ErrsEq(err, tc.expectedErr) {
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
	expectedRbErr := atomictest.NewError(nil, rbErr)

	createErr := errors.New("savepoint error")
	expectedCreateErr := atomictest.NewError(nil, createErr)

	selectErr := errors.New("select 2 error")
	expectedSelectErr := atomictest.NewError(selectErr, nil)

	releaseErr := errors.New("release error")
	expectedReleaseErr := atomictest.NewError(selectErr, releaseErr)

	savepointer := func(release bool) *mock.Savepointer {
		return mock.NewSavepointer(ioutil.Discard, release)
	}

	testCases := []struct {
		name        string
		savepointer *mock.Savepointer
		mocker      func(sqlmock.Sqlmock) sqlmock.Sqlmock
		expectedErr *atomic.Error
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
			q, err := atomic.NewQuerier(ctx, db, tc.savepointer, sql.TxOptions{})
			if err != nil {
				t.Fatal("Error creating Querier:", err)
			}
			if err := q.Atomic(func(ctx context.Context, q atomic.Querier) error {
				var dummy int
				if err := q.QueryRowContext(ctx, "SELECT 1;").Scan(&dummy); err != nil {
					t.Log("error in select 1:", err)
					return err
				}
				if err := q.Atomic(func(ctx context.Context, q atomic.Querier) error {
					return q.QueryRowContext(ctx, "SELECT 2;").Scan(&dummy)
				}); !atomictest.ErrsEq(err, tc.expectedErr) {
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
	beginErr := errors.New("begin error")

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
		txCreator   atomic.TxCreator
		expectedErr error
	}{
		{name: "nil db", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock { return m },
			getDb: func() (*sql.DB, sqlmock.Sqlmock) {
				_, _sqlmock := getDb()
				return nil, _sqlmock
			}, savepointer: mock.NewSavepointer(ioutil.Discard, true), txCreator: atomic.DefaultTxCreator,
			expectedErr: atomic.ErrNeedsDb},
		{name: "nil savepointer", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock { return m },
			getDb: getDb, savepointer: nil, txCreator: atomic.DefaultTxCreator,
			expectedErr: atomic.ErrNeedsSavepointer},
		{name: "begin err", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin().WillReturnError(beginErr)
			return m
		}, getDb: getDb, savepointer: mock.NewSavepointer(ioutil.Discard, true), txCreator: atomic.DefaultTxCreator,
			expectedErr: beginErr},
		{name: "success", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			return m
		}, getDb: getDb, savepointer: mock.NewSavepointer(ioutil.Discard, true), txCreator: atomic.DefaultTxCreator,
			expectedErr: nil},
		{name: "success - nil TxCreator", mocker: func(m sqlmock.Sqlmock) sqlmock.Sqlmock {
			m.ExpectBegin()
			return m
		}, getDb: getDb, savepointer: mock.NewSavepointer(ioutil.Discard, true), txCreator: nil, expectedErr: nil},
	}

	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, _sqlmock := tc.getDb()
			_sqlmock = tc.mocker(_sqlmock)

			if _, err := atomic.NewQuerierWithTxCreator(ctx, db, tc.savepointer,
				sql.TxOptions{}, tc.txCreator); err != tc.expectedErr {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}

			if err := _sqlmock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}
