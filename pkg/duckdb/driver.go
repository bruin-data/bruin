//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/columnar-tech/dbc"
	"github.com/columnar-tech/dbc/config"
	"github.com/gofrs/flock"
)

var (
	driverOnce         sync.Once
	errDriverInit      error
	quackDriverOnce    sync.Once
	errQuackDriverInit error
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

// EnsureQuackADBCDriverInstalled installs the DuckDB driver used by Quack
// connections into its own directory, leaving the default driver untouched.
func EnsureQuackADBCDriverInstalled(ctx context.Context) error {
	quackDriverOnce.Do(func() {
		errQuackDriverInit = ensureQuackDriverInstalledInternal(ctx)
	})
	return errQuackDriverInit
}

// InstallDrivers installs both DuckDB drivers and the quack extension ahead of
// use, so images can include them instead of downloading them on every run.
func InstallDrivers(ctx context.Context) error {
	if err := EnsureADBCDriverInstalled(ctx); err != nil {
		return err
	}
	if err := EnsureQuackADBCDriverInstalled(ctx); err != nil {
		return err
	}

	dir, err := QuackADBCDriverDir()
	if err != nil {
		return err
	}
	var driver drivermgr.Driver
	db, err := driver.NewDatabase(map[string]string{"driver": quackDriverPath(dir), "path": ":memory:"})
	if err != nil {
		return fmt.Errorf("failed to open the Quack DuckDB driver: %w", err)
	}
	defer db.Close()
	conn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open the Quack DuckDB driver: %w", err)
	}
	defer conn.Close()
	if err := execADBCStatement(ctx, conn, "INSTALL quack"); err != nil {
		return fmt.Errorf("failed to install the quack extension: %w", err)
	}
	return nil
}

func ensureDriverInstalledInternal(ctx context.Context) error {
	cfg, err := user.EnsureADBCDriverConfig(dbcDriverName)
	if err != nil {
		return err
	}
	driverPath, err := user.ADBCDriverPath(dbcDriverName)
	if err != nil {
		return err
	}
	return ensureDriver(ctx, cfg, driverPath, duckdbDriverVersion)
}

func ensureQuackDriverInstalledInternal(ctx context.Context) error {
	dir, err := QuackADBCDriverDir()
	if err != nil {
		return err
	}
	cfg := config.Config{Level: config.ConfigEnv, Location: dir}
	return ensureDriver(ctx, cfg, quackDriverPath(dir), QuackDriverVersion)
}

func quackDriverPath(dir string) string {
	return filepath.Join(dir, dbcDriverName+".toml")
}

func ensureDriver(ctx context.Context, cfg config.Config, driverPath, version string) error {
	if err := tryLoadDriver(driverPath); err == nil { //nolint:contextcheck
		return nil
	}

	return withDriverInstallLock(ctx, cfg, func() error {
		// Another process may have installed the driver while we waited.
		if err := tryLoadDriver(driverPath); err == nil { //nolint:contextcheck
			return nil
		}

		if err := installDuckDBADBCDriver(ctx, cfg, version); err != nil {
			return err
		}

		if err := tryLoadDriver(driverPath); err != nil { //nolint:contextcheck
			return fmt.Errorf("DuckDB ADBC driver %s still not available after installation: %w", version, err)
		}

		return nil
	})
}

func withDriverInstallLock(ctx context.Context, cfg config.Config, install func() error) error {
	location, err := config.EnsureLocation(cfg)
	if err != nil {
		return fmt.Errorf("failed to prepare DuckDB ADBC driver directory: %w", err)
	}

	// Keep the lock outside the version directory removed on verification failure.
	// Leave it in place so all processes continue to lock the same file.
	lock := flock.New(filepath.Join(location, ".duckdb-install.lock"))
	defer func() { _ = lock.Close() }()
	if locked, err := lock.TryLockContext(ctx, 100*time.Millisecond); err != nil {
		return fmt.Errorf("failed to lock DuckDB ADBC driver installation: %w", err)
	} else if !locked {
		return errors.New("failed to lock DuckDB ADBC driver installation")
	}

	return install()
}

func installDuckDBADBCDriver(ctx context.Context, cfg config.Config, version string) error {
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

	constraint, err := semver.NewConstraint("=" + version)
	if err != nil {
		return fmt.Errorf("failed to build dbc driver version constraint: %w", err)
	}

	pkg, err := duckDBDriver.GetWithConstraint(constraint, config.PlatformTuple())
	if err != nil {
		return fmt.Errorf("failed to resolve dbc driver %s=%s: %w", dbcDriverName, version, err)
	}

	downloaded, cleanup, err := downloadDBCDriverPackage(ctx, client, pkg, version)
	if err != nil {
		return fmt.Errorf("failed to download dbc driver %s=%s: %w", dbcDriverName, version, err)
	}
	defer func() {
		_ = downloaded.Close()
		cleanup()
	}()

	manifest, err := config.InstallDriver(cfg, dbcDriverName, downloaded)
	if err != nil {
		return fmt.Errorf("failed to install dbc driver %s=%s: %w", dbcDriverName, version, err)
	}

	if err := verifyDBCDriverSignature(manifest); err != nil {
		driverDir := filepath.Dir(manifest.Driver.Shared.Get(config.PlatformTuple()))
		_ = os.RemoveAll(driverDir)
		return fmt.Errorf("failed to verify dbc driver %s=%s: %w", dbcDriverName, version, err)
	}

	if err := config.CreateManifest(cfg, manifest.DriverInfo); err != nil {
		return fmt.Errorf("failed to create dbc manifest for %s=%s: %w", dbcDriverName, version, err)
	}

	return nil
}

func downloadDBCDriverPackage(ctx context.Context, client *dbc.Client, pkg dbc.PkgInfo, version string) (*os.File, func(), error) {
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

	filename := dbcDriverName + "-" + version + ".tar.gz"
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

func tryLoadDriver(driverPath string) error {
	var driver drivermgr.Driver
	db, err := driver.NewDatabase(map[string]string{"driver": driverPath, "path": ":memory:"})
	if err != nil {
		return fmt.Errorf("failed to open duckdb adbc driver: %w", err)
	}
	defer db.Close()

	conn, err := db.Open(context.Background())
	if err != nil {
		return fmt.Errorf("failed to ping duckdb adbc driver: %w", err)
	}
	return conn.Close()
}
