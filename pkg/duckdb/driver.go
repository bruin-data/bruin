//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/Masterminds/semver/v3"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/columnar-tech/dbc"
	"github.com/columnar-tech/dbc/config"
)

var (
	driverOnce    sync.Once
	errDriverInit error
)

const (
	adbcDriverName = "adbc_duckdb"
	dbcDriverName  = "duckdb"
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
		return nil
	}

	if err := installDuckDBADBCDriver(ctx); err != nil {
		return err
	}

	if err := tryLoadDriver(); err != nil { //nolint:contextcheck
		return fmt.Errorf("DuckDB ADBC driver still not available after installation: %w", err)
	}

	return nil
}

func installDuckDBADBCDriver(ctx context.Context) error {
	client, err := dbc.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create dbc client: %w", err)
	}

	drivers, err := client.Search(ctx, dbcDriverName)
	if err != nil {
		return fmt.Errorf("failed to search for dbc driver %s: %w", dbcDriverName, err)
	}

	var duckDBDriver *dbc.Driver
	for i := range drivers {
		if drivers[i].Path == dbcDriverName {
			duckDBDriver = &drivers[i]
			break
		}
	}
	if duckDBDriver == nil {
		return fmt.Errorf("dbc driver %q not found", dbcDriverName)
	}

	constraint, err := semver.NewConstraint("=" + duckdbDriverVersion)
	if err != nil {
		return fmt.Errorf("failed to build dbc driver version constraint: %w", err)
	}

	pkg, err := duckDBDriver.GetWithConstraint(constraint, config.PlatformTuple())
	if err != nil {
		return fmt.Errorf("failed to resolve dbc driver %s=%s: %w", dbcDriverName, duckdbDriverVersion, err)
	}

	downloaded, cleanup, err := downloadDBCDriverPackage(ctx, client, pkg)
	if err != nil {
		return fmt.Errorf("failed to download dbc driver %s=%s: %w", dbcDriverName, duckdbDriverVersion, err)
	}
	defer func() {
		_ = downloaded.Close()
		cleanup()
	}()

	cfg := client.GetConfig(config.ConfigUser)
	manifest, err := config.InstallDriver(cfg, dbcDriverName, downloaded)
	if err != nil {
		return fmt.Errorf("failed to install dbc driver %s=%s: %w", dbcDriverName, duckdbDriverVersion, err)
	}

	if err := verifyDBCDriverSignature(manifest); err != nil {
		driverDir := filepath.Dir(manifest.Driver.Shared.Get(config.PlatformTuple()))
		_ = os.RemoveAll(driverDir)
		return fmt.Errorf("failed to verify dbc driver %s=%s: %w", dbcDriverName, duckdbDriverVersion, err)
	}

	if err := config.CreateManifest(cfg, manifest.DriverInfo); err != nil {
		return fmt.Errorf("failed to create dbc manifest for %s=%s: %w", dbcDriverName, duckdbDriverVersion, err)
	}

	return nil
}

func downloadDBCDriverPackage(ctx context.Context, client *dbc.Client, pkg dbc.PkgInfo) (*os.File, func(), error) {
	body, err := client.Download(ctx, pkg)
	if err != nil {
		return nil, nil, err
	}
	defer body.Close()

	tmpdir, err := os.MkdirTemp("", "adbc-drivers-*")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(tmpdir)
	}

	filename := dbcDriverName + "-" + duckdbDriverVersion + ".tar.gz"
	if pkg.Path != nil {
		if base := path.Base(pkg.Path.Path); base != "." && base != "/" {
			filename = base
		}
	}

	downloaded, err := os.Create(filepath.Join(tmpdir, filename))
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := io.Copy(downloaded, body); err != nil {
		_ = downloaded.Close()
		cleanup()
		return nil, nil, fmt.Errorf("failed to write temp file: %w", err)
	}

	if _, err := downloaded.Seek(0, io.SeekStart); err != nil {
		_ = downloaded.Close()
		cleanup()
		return nil, nil, fmt.Errorf("failed to rewind temp file: %w", err)
	}

	return downloaded, cleanup, nil
}

func verifyDBCDriverSignature(manifest config.Manifest) error {
	if manifest.Files.Driver == "" {
		return nil
	}

	driverDir := filepath.Dir(manifest.Driver.Shared.Get(config.PlatformTuple()))
	lib, err := os.Open(filepath.Join(driverDir, manifest.Files.Driver))
	if err != nil {
		return fmt.Errorf("could not open driver file: %w", err)
	}
	defer lib.Close()

	sigFile := manifest.Files.Signature
	if sigFile == "" {
		sigFile = manifest.Files.Driver + ".sig"
	}

	sig, err := os.Open(filepath.Join(driverDir, sigFile))
	if err != nil {
		return fmt.Errorf("failed to open signature file: %w", err)
	}
	defer sig.Close()

	//nolint:staticcheck
	if err := dbc.SignedByColumnar(lib, sig); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	return nil
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
