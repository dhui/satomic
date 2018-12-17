package sqlx_test

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
	"github.com/dhui/satomic"
	"github.com/dhui/satomic/savepointers"
	"github.com/dhui/satomic/savepointers/mock"
	asqlx "github.com/dhui/satomic/sqlx"
)

func TestNewQuerier(t *testing.T) {
	noopMocker := func(m sqlmock.Sqlmock) sqlmock.Sqlmock { return m }

	getDb := func() (*sqlx.DB, sqlmock.Sqlmock) {
		db, _sqlmock, err := sqlmock.New()
		if err != nil {
			t.Fatal("Error creating sqlmock:", err)
		}
		return sqlx.NewDb(db, ""), _sqlmock
	}

	testCases := []struct {
		name        string
		mocker      func(sqlmock.Sqlmock) sqlmock.Sqlmock
		getDb       func() (*sqlx.DB, sqlmock.Sqlmock)
		savepointer savepointers.Savepointer
		expectedErr error
	}{
		{name: "nil db", mocker: noopMocker, getDb: func() (*sqlx.DB, sqlmock.Sqlmock) {
			_, _sqlmock := getDb()
			return nil, _sqlmock
		}, savepointer: mock.NewSavepointer(ioutil.Discard, true), expectedErr: satomic.ErrNeedsDb},
		{name: "nil savepointer", mocker: noopMocker, getDb: getDb, savepointer: nil,
			expectedErr: satomic.ErrNeedsSavepointer},
		{name: "success", mocker: noopMocker, getDb: getDb, savepointer: mock.NewSavepointer(ioutil.Discard, true),
			expectedErr: nil},
	}

	ctx := context.Background()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, _sqlmock := tc.getDb()
			_sqlmock = tc.mocker(_sqlmock)

			if _, err := asqlx.NewQuerier(ctx, db, tc.savepointer,
				sql.TxOptions{}); err != tc.expectedErr {
				t.Errorf("Didn't get the expected error: %+v != %+v", err, tc.expectedErr)
			}

			if err := _sqlmock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}
