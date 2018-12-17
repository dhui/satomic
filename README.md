# satomic

[![Build Status](https://img.shields.io/travis/dhui/satomic/master.svg)](https://travis-ci.org/dhui/satomic) [![Code Coverage](https://img.shields.io/codecov/c/github/dhui/satomic.svg)](https://codecov.io/gh/dhui/satomic) [![GoDoc](https://godoc.org/github.com/dhui/satomic?status.svg)](https://godoc.org/github.com/dhui/satomic) [![Go Report Card](https://goreportcard.com/badge/github.com/dhui/satomic)](https://goreportcard.com/report/github.com/dhui/satomic) [![GitHub Release](https://img.shields.io/github/release/dhui/satomic/all.svg)](https://github.com/dhui/satomic/releases) ![Supported Go versions](https://img.shields.io/badge/Go-1.11-lightgrey.svg)

satomic is a Golang package that makes managing nested SQL transactions/savepoints easier

## Example usage

```golang
package main

import (
    "context"
    "database/sql"
    "github.com/dhui/satomic"
    "github.com/dhui/satomic/savepointers/postgres"
)

// Error handling omitted for brevity. Actual code should handle errors
func main() {
    ctx := context.Background()
    var db *sql.DB  // Use an actual db

    // savepointer should match the db driver used
    q, _ := satomic.NewQuerier(ctx, db, postgres.Savepointer{}, sql.TxOptions{})

    q.Atomic(func(ctx context.Context, q satomic.Querier) error {
        // In transaction
        var dummy int
        q.QueryRowContext(ctx, "SELECT 1;").Scan(&dummy)

        q.Atomic(func(ctx context.Context, q satomic.Querier) error {
            // In first savepoint
            return q.QueryRowContext(ctx, "SELECT 2;").Scan(&dummy)
        })

        q.Atomic(func(ctx context.Context, q satomic.Querier) error {
            // In second savepoint
            q.QueryRowContext(ctx, "SELECT 3;").Scan(&dummy)

            q.Atomic(func(ctx context.Context, q satomic.Querier) error {
                // In third savepoint
                q.QueryRowContext(ctx, "SELECT 4;").Scan(&dummy)
                return nil
            })

            return nil
        })

        return nil
    })
}
```

A more complete example can be found in the [GoDoc](https://godoc.org/github.com/dhui/satomic)

## What's with the name?
Go **S**QL **atomic** => satomic

---

Inspired by [sqlexp](https://github.com/golang-sql/sqlexp) and Django's [atomic](https://docs.djangoproject.com/en/2.1/topics/db/transactions/#django.db.transaction.atomic) decorator/context manager.