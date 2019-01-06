package mssql_test

import (
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

var msSQLDBGetter savepointertest.DBGetter = func(c dktest.ContainerInfo) (*sql.DB, error) {
	connStr := fmt.Sprintf("sqlserver://sa:%s@%s:%s", password, c.IP, c.Port)
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func TestSavepointerMSSQL(t *testing.T) {
	t.Parallel()

	// https://www.mysql.com/support/supportedplatforms/database.html
	versions := []string{
		"microsoft/mssql-server-linux",
	}

	savepointertest.TestSavepointerWithDocker(t, mssql.Savepointer{}, versions, dktest.Options{Env: env,
		PortRequired: true, ReadyFunc: msSQLDBGetter.ReadyFunc(), Timeout: timeout}, msSQLDBGetter)
}
