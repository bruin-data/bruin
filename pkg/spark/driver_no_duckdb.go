//go:build bruin_no_duckdb

package spark

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/pkg/errors"
)

const (
	adbcDriverName = "adbc_spark"
	dbcDriverName  = "spark"
)

func ADBCDriverName() string {
	return adbcDriverName
}

func EnsureADBCDriverInstalled(context.Context) error {
	return errors.New("Spark ADBC support requires a CGO-enabled Bruin build")
}

func newADBCDatabase(map[string]string) (adbc.Database, error) { //nolint:ireturn
	return nil, errors.New("Spark ADBC support requires a CGO-enabled Bruin build")
}
