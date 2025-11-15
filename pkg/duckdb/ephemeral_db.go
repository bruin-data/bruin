//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
)

// DriverInstaller is an interface for installing the DuckDB ADBC driver.
type DriverInstaller interface {
	InstallDuckDBDriver(ctx context.Context) error
}

var (
	registerDriverOnce sync.Once
	driverInstaller    DriverInstaller
)

// SetDriverInstaller sets a custom driver installer (useful for testing).
func SetDriverInstaller(customInstaller DriverInstaller) {
	driverInstaller = customInstaller
}

// registerDriver registers the DuckDB ADBC driver with database/sql.
func registerDriver() {
	registerDriverOnce.Do(func() {
		// Register the DuckDB ADBC driver with database/sql
		sql.Register("duckdb", sqldriver.Driver{Driver: &drivermgr.Driver{}})
	})
}

// NewEphemeralConnection creates a new database/sql connection using the ADBC sqldriver package.
func NewEphemeralConnection(c DuckDBConfig) (*sql.DB, error) {
	// Register the driver
	registerDriver()

	// Build the DSN with driver parameter
	dsn := fmt.Sprintf("driver=duckdb;path=%s", c.ToDBConnectionURI())

	// Use sqldriver to open a database/sql connection
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	return db, nil
}
