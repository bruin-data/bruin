//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/bruin-data/bruin/pkg/uv"
)

var (
	driverOnce    sync.Once
	errDriverInit error
	uvChecker     = &uv.Checker{}
)

const adbcDriverName = "adbc_duckdb"

//nolint:gochecknoinits
func init() {
	sql.Register(adbcDriverName, sqldriver.Driver{Driver: &drivermgr.Driver{}})
}

// ADBCDriverName returns the registered SQL driver name for ADBC DuckDB.
func ADBCDriverName() string {
	return adbcDriverName
}

func EnsureADBCDriverInstalled(ctx context.Context) error {
	driverOnce.Do(func() {
		errDriverInit = ensureDriverInstalledInternal(ctx)
	})
	return errDriverInit
}

func ensureDriverInstalledInternal(ctx context.Context) error {
	if err := tryLoadDriver(); err == nil {
		return nil
	}

	uvPath, err := uvChecker.EnsureUvInstalled(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure uv is installed: %w", err)
	}

	cmd := exec.CommandContext(ctx, uvPath, "tool", "install", "--quiet", "--no-config", "dbc")
	cmd.Stdout = nil
	cmd.Stderr = nil
	_ = cmd.Run()

	cmd = exec.CommandContext(ctx, uvPath, "tool", "run", "--no-config", "dbc", "install", "duckdb")
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("dbc install duckdb failed: %w", err)
	}

	if err := tryLoadDriver(); err != nil {
		return fmt.Errorf("DuckDB ADBC driver still not available after installation: %w", err)
	}

	return nil
}

func tryLoadDriver() error {
	db, err := sql.Open("adbc_duckdb", "driver=duckdb;path=:memory:")
	if err != nil {
		return fmt.Errorf("failed to open duckdb adbc driver: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping duckdb adbc driver: %w", err)
	}

	return nil
}
