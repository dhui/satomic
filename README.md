# satomic

[![Build Status](https://img.shields.io/travis/dhui/satomic/master.svg)](https://travis-ci.org/dhui/satomic) [![Code Coverage](https://img.shields.io/codecov/c/github/dhui/satomic.svg)](https://codecov.io/gh/dhui/satomic) [![GoDoc](https://godoc.org/github.com/dhui/satomic?status.svg)](https://godoc.org/github.com/dhui/satomic) [![Go Report Card](https://goreportcard.com/badge/github.com/dhui/satomic)](https://goreportcard.com/report/github.com/dhui/satomic) [![GitHub Release](https://img.shields.io/github/release/dhui/satomic/all.svg)](https://github.com/dhui/satomic/releases) ![Supported Go versions](https://img.shields.io/badge/Go-1.11-lightgrey.svg)

satomic is a Golang package that makes managing nested SQL transactions/savepoints easier

## Overview

Create a `Querier` and use the `Atomic()` method. Any SQL statements inside the `Atomic()` method's callback function will be appropriately wrapped in a transaction or savepoint. Transaction and savepoint management will be handled for you automatically. Any error returned by the callback function (or unrecovered panic) will rollback the savepoint or transaction accordingly. A new `Querier` instance is also provided to the `Atomic()` method's callback function to allow nesting savepoints.

## Status

satomic is **not stable yet**, so the **interfaces may change in a non-backwards compatible manner**.
satomic follows [Semantic Versioning 2.0.0](https://semver.org/). While the major version number is `0`, all backwards compatible changes will be denoted by a minor version number bump. All other changes will increase the patch version number.

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

For usage with [sqlx](https://github.com/jmoiron/sqlx), use [github.com/dhui/satomic/sqlx](https://godoc.org/github.com/dhui/satomic/sqlx)

## What's with the name?

Go **S**QL **atomic** => satomic

---

Inspired by [sqlexp](https://github.com/golang-sql/sqlexp) and Django's [atomic](https://docs.djangoproject.com/en/2.1/topics/db/transactions/#django.db.transaction.atomic) decorator/context manager.
