package mssql_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

import (
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/dhui/dktest"
)

import (
	"github.com/dhui/satomic/savepointers/mssql"
	"github.com/dhui/satomic/savepointers/savepointertest"
)

const (
	password = "insecurePassword1"
	timeout  = 3 * time.Minute
)

var env = map[string]string{
	// Developer edition (the default) is free
	// License: https://go.microsoft.com/fwlink/?linkid=857698
	"ACCEPT_EULA": "Y",
	"SA_PASSWORD": password,
}

var msSQLDBGetter savepointertest.DBGetter = func(ctx context.Context, c dktest.ContainerInfo) (*sql.DB, error) {
	ip, port, err := c.FirstPort()
	if err != nil {
		return nil, err
	}
	connStr := fmt.Sprintf("sqlserver://sa:%s@%s:%s", password, ip, port)
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

func TestSavepointerMSSQL(t *testing.T) {
	t.Parallel()

	versions := []string{
		"mcr.microsoft.com/mssql/server:2017-latest-ubuntu",
	}

	savepointertest.TestSavepointerWithDocker(t, mssql.Savepointer{}, versions, dktest.Options{Env: env,
		PortRequired: true, ReadyFunc: msSQLDBGetter.ReadyFunc(), Timeout: timeout}, msSQLDBGetter)
}
