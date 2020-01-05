package satomicx_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
)

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

import (
	satomic "github.com/dhui/satomic/satomicx"
	"github.com/dhui/satomic/savepointers/mock"
)

func Example() {
	db, _sqlmock, err := sqlmock.New()
	if err != nil {
		fmt.Println("Error creating sqlmock:", err)
		return
	}
	defer db.Close() // nolint:errcheck

	type testSruct struct{ ID int }

	_sqlmock.ExpectBegin()
	_sqlmock.ExpectQuery("SELECT 1;").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	_sqlmock.ExpectExec("SAVEPOINT 1;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectQuery("SELECT 2;").WillReturnError(errors.New("select 2 error"))
	_sqlmock.ExpectExec("ROLLBACK TO 1;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectExec("SAVEPOINT 2;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectQuery("SELECT 3;").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(3))
	_sqlmock.ExpectExec("RELEASE 2;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectExec("SAVEPOINT 3;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectQuery("SELECT 4;").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(4))
	_sqlmock.ExpectExec("SAVEPOINT 4;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectQuery("SELECT 5;").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))
	_sqlmock.ExpectExec("SAVEPOINT 5;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectQuery("SELECT 6;").WillReturnError(errors.New("select 6 error"))
	_sqlmock.ExpectExec("ROLLBACK TO 5;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectExec("RELEASE 4;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectExec("RELEASE 3;").WillReturnResult(sqlmock.NewResult(0, 0))
	_sqlmock.ExpectCommit()

	ctx := context.Background()
	// For actual code, use a real Savepointer instead of a mocked one
	q, err := satomic.NewQuerier(ctx, sqlx.NewDb(db, ""), mock.NewSavepointer(os.Stdout, true), sql.TxOptions{})
	if err != nil {
		fmt.Println("Error creating Querier:", err)
		return
	}
	if err := q.Atomicx(func(ctx context.Context, q satomic.Querier) error {
		var dummy testSruct
		if err := q.GetContext(ctx, &dummy, "SELECT 1;"); err != nil {
			fmt.Println(err)
		}
		// SAVEPOINT 1
		if err := q.Atomicx(func(ctx context.Context, q satomic.Querier) error {
			return q.GetContext(ctx, &dummy, "SELECT 2;")
		}); err != nil {
			fmt.Println(err)
		}
		// SAVEPOINT 2
		if err := q.Atomicx(func(ctx context.Context, q satomic.Querier) error {
			return q.GetContext(ctx, &dummy, "SELECT 3;")
		}); err != nil {
			fmt.Println(err)
		}
		// SAVEPOINT 3
		if err := q.Atomicx(func(ctx context.Context, q satomic.Querier) error {
			if err := q.GetContext(ctx, &dummy, "SELECT 4;"); err != nil {
				fmt.Println(err)
			}
			// SAVEPOINT 4
			if err := q.Atomicx(func(ctx context.Context, q satomic.Querier) error {
				if err := q.GetContext(ctx, &dummy, "SELECT 5;"); err != nil {
					fmt.Println(err)
				}
				// SAVEPOINT 5
				if err := q.Atomicx(func(ctx context.Context, q satomic.Querier) error {
					return q.GetContext(ctx, &dummy, "SELECT 6;")
				}); err != nil {
					fmt.Println(err)
				}
				return nil
			}); err != nil {
				fmt.Println(err)
			}
			return nil
		}); err != nil {
			fmt.Println(err)
		}
		return nil
	}); err != nil {
		fmt.Println(err)
	}

	if err := _sqlmock.ExpectationsWereMet(); err != nil {
		fmt.Println(err)
	}

	// Output:
	// SAVEPOINT 1;
	// ROLLBACK TO 1;
	// Err: "select 2 error" Atomic: <nil>
	// SAVEPOINT 2;
	// RELEASE 2;
	// SAVEPOINT 3;
	// SAVEPOINT 4;
	// SAVEPOINT 5;
	// ROLLBACK TO 5;
	// Err: "select 6 error" Atomic: <nil>
	// RELEASE 4;
	// RELEASE 3;
}
