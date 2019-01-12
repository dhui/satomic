package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

import (
	"github.com/dhui/dktest"
	_ "github.com/go-sql-driver/mysql"
)

import (
	"github.com/dhui/satomic/savepointers/mysql"
	"github.com/dhui/satomic/savepointers/savepointertest"
)

const (
	password = "insecurepassword"
	db       = "public"
	timeout  = 3 * time.Minute
)

var env = map[string]string{
	"MYSQL_ROOT_PASSWORD": password,
	"MYSQL_DATABASE":      db,
}

var mySQLDBGetter savepointertest.DBGetter = func(ctx context.Context, c dktest.ContainerInfo) (*sql.DB, error) {
	ip, port, err := c.Port(3306)
	if err != nil {
		return nil, err
	}
	connStr := fmt.Sprintf("root:%s@tcp(%s:%s)/%s", password, ip, port, db)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

func TestSavepointerMySQL(t *testing.T) {
	t.Parallel()

	// https://www.mysql.com/support/supportedplatforms/database.html
	versions := []string{
		"mysql:8",
		"mysql:5.7",
		"mysql:5.6",
		"mysql:5.5",
	}

	savepointertest.TestSavepointerWithDocker(t, mysql.Savepointer{}, versions, dktest.Options{Env: env,
		PortRequired: true, ReadyFunc: mySQLDBGetter.ReadyFunc(), Timeout: timeout}, mySQLDBGetter)
}
