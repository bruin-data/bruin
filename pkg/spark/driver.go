//go:build !bruin_no_duckdb

package spark

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
	"github.com/apache/arrow-adbc/go/adbc"
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
	adbcDriverName     = "adbc_spark"
	dbcDriverName      = "spark"
	sparkDriverVersion = "0.1.0"
)

//nolint:gochecknoinits
func init() {
	sql.Register(adbcDriverName, sqldriver.Driver{Driver: &drivermgr.Driver{}})
}

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
	if sparkDriverVersionIsActive() {
		if err := tryLoadDriver(); err == nil {
			return nil
		}
	}
	if err := installSparkADBCDriver(ctx); err != nil {
		return err
	}
	if !sparkDriverVersionIsActive() {
		return fmt.Errorf(
			"spark ADBC driver %s was installed, but another version takes precedence in the ADBC driver search path",
			sparkDriverVersion,
		)
	}
	if err := tryLoadDriver(); err != nil {
		return fmt.Errorf("spark ADBC driver still not available after installation: %w", err)
	}
	return nil
}

func sparkDriverVersionIsActive() bool {
	configs := config.Get()
	for _, level := range []config.ConfigLevel{
		config.ConfigEnv,
		config.ConfigUser,
		config.ConfigSystem,
	} {
		cfg, ok := configs[level]
		if !ok {
			continue
		}
		driver, err := config.GetDriver(cfg, dbcDriverName)
		if err != nil {
			continue
		}
		return driver.Version != nil && driver.Version.String() == sparkDriverVersion
	}
	return false
}

func newADBCDatabase(options map[string]string) (adbc.Database, error) { //nolint:ireturn
	var driver drivermgr.Driver
	return driver.NewDatabase(options)
}

func tryLoadDriver() error {
	database, err := newADBCDatabase(map[string]string{
		"driver": dbcDriverName,
		"uri":    "spark://localhost:1?auth_type=none&api=connect",
	})
	if err != nil {
		return fmt.Errorf("failed to load Spark ADBC driver: %w", err)
	}
	return database.Close()
}

func installSparkADBCDriver(ctx context.Context) error {
	client, err := dbc.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create dbc client: %w", err)
	}

	drivers, err := client.Search(ctx, dbcDriverName)
	if err != nil {
		return fmt.Errorf("failed to search for dbc driver %s: %w", dbcDriverName, err)
	}

	var sparkDriver *dbc.Driver
	for i := range drivers {
		if drivers[i].Path == dbcDriverName {
			sparkDriver = &drivers[i]
			break
		}
	}
	if sparkDriver == nil {
		return fmt.Errorf("dbc driver %q not found", dbcDriverName)
	}

	constraint, err := semver.NewConstraint("=" + sparkDriverVersion)
	if err != nil {
		return fmt.Errorf("failed to build dbc driver version constraint: %w", err)
	}
	pkg, err := sparkDriver.GetWithConstraint(constraint, config.PlatformTuple())
	if err != nil {
		return fmt.Errorf("failed to resolve dbc driver %s=%s: %w", dbcDriverName, sparkDriverVersion, err)
	}

	downloaded, cleanup, err := downloadDBCDriverPackage(ctx, client, pkg)
	if err != nil {
		return fmt.Errorf("failed to download dbc driver %s=%s: %w", dbcDriverName, sparkDriverVersion, err)
	}
	defer func() {
		_ = downloaded.Close()
		cleanup()
	}()

	cfg := config.Config{
		Level:    config.ConfigUser,
		Location: config.ConfigUser.ConfigLocation(),
	}
	manifest, err := config.InstallDriver(cfg, dbcDriverName, downloaded)
	if err != nil {
		return fmt.Errorf("failed to install dbc driver %s=%s: %w", dbcDriverName, sparkDriverVersion, err)
	}

	if err := verifyDBCDriverSignature(manifest); err != nil {
		driverDir := filepath.Dir(manifest.Driver.Shared.Get(config.PlatformTuple()))
		_ = os.RemoveAll(driverDir)
		return fmt.Errorf("failed to verify dbc driver %s=%s: %w", dbcDriverName, sparkDriverVersion, err)
	}
	if err := config.CreateManifest(cfg, manifest.DriverInfo); err != nil {
		return fmt.Errorf("failed to create dbc manifest for %s=%s: %w", dbcDriverName, sparkDriverVersion, err)
	}
	return nil
}

func downloadDBCDriverPackage(ctx context.Context, client *dbc.Client, pkg dbc.PkgInfo) (*os.File, func(), error) {
	body, err := client.Download(ctx, pkg)
	if err != nil {
		return nil, nil, err
	}
	defer body.Close()

	tmpdir, err := os.MkdirTemp("", "adbc-spark-*")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(tmpdir)
	}

	filename := dbcDriverName + "-" + sparkDriverVersion + ".tar.gz"
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
