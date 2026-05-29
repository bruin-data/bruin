//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"strings"
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

const (
	adbcDriverName = "adbc_duckdb"
	// duckdbDriverVersion pins the DuckDB ADBC driver version installed via dbc.
	// MotherDuck currently supports up to v1.5.2; newer DuckDB releases are rejected.
	duckdbDriverVersion = "1.5.2"
)

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
	if err := tryLoadDriver(); err == nil { //nolint:contextcheck
		if version, err := installedDuckDBVersion(); err == nil && version == duckdbDriverVersion { //nolint:contextcheck
			return nil
		}
		// Driver loads but version doesn't match the pinned one; fall through to reinstall.
	}

	uvPath, err := uvChecker.EnsureUvInstalled(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure uv is installed: %w", err)
	}

	cmd := exec.CommandContext(ctx, uvPath, "tool", "install", "--quiet", "--no-config", "dbc")
	cmd.Stdout = nil
	cmd.Stderr = nil
	_ = cmd.Run()

	// Remove any previously installed duckdb driver so the pinned version replaces it.
	// Ignore errors: the driver may not be present at the user level.
	cmd = exec.CommandContext(ctx, uvPath, "tool", "run", "--no-config", "dbc", "uninstall", "--quiet", "--level", "user", "duckdb")
	cmd.Stdout = nil
	cmd.Stderr = nil
	_ = cmd.Run()

	cmd = exec.CommandContext(ctx, uvPath, "tool", "run", "--no-config", "dbc", "install", "--level", "user", "duckdb="+duckdbDriverVersion)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("dbc install duckdb failed: %w", err)
	}

	if err := tryLoadDriver(); err != nil { //nolint:contextcheck
		return fmt.Errorf("DuckDB ADBC driver still not available after installation: %w", err)
	}

	return nil
}

// installedDuckDBVersion queries the loaded DuckDB driver for its version string,
// returning it without the leading "v" (e.g. "1.5.2").
func installedDuckDBVersion() (string, error) {
	db, err := sql.Open(adbcDriverName, "driver=duckdb;path=:memory:")
	if err != nil {
		return "", err
	}
	defer db.Close()

	var version string
	if err := db.QueryRowContext(context.Background(), "SELECT version()").Scan(&version); err != nil {
		return "", err
	}
	return strings.TrimPrefix(strings.TrimSpace(version), "v"), nil
}

func tryLoadDriver() error {
	db, err := sql.Open("adbc_duckdb", "driver=duckdb;path=:memory:")
	if err != nil {
		return fmt.Errorf("failed to open duckdb adbc driver: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		return fmt.Errorf("failed to ping duckdb adbc driver: %w", err)
	}

	return nil
}
