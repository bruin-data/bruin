package user

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/columnar-tech/dbc/config"
	"github.com/spf13/afero"
)

// ADBCDriverPath selects Bruin's manifest without changing the process-wide
// ADBC search path. With no override, the driver manager keeps its usual lookup.
func ADBCDriverPath(name string) (string, error) {
	if os.Getenv("BRUIN_HOME") == "" {
		return name, nil
	}
	c := NewConfigManager(nil)
	if err := c.ensureHomeDirSet(); err != nil {
		return "", err
	}
	return filepath.Join(c.bruinHomeDir, "adbc", name+".toml"), nil
}

// EnsureADBCDriverConfig relocates Bruin-managed drivers on first use. ConfigEnv
// uses filesystem manifests on Windows too, rather than the user registry.
func EnsureADBCDriverConfig(name string) (config.Config, error) {
	legacy := config.Config{Level: config.ConfigUser, Location: config.ConfigUser.ConfigLocation()}
	if os.Getenv("BRUIN_HOME") == "" {
		return legacy, nil
	}
	home, err := NewConfigManager(nil).EnsureAndGetBruinHomeDir()
	if err != nil {
		return config.Config{}, err
	}
	cfg := config.Config{Level: config.ConfigEnv, Location: filepath.Join(home, "adbc")}
	homeMigrationMu.Lock()
	defer homeMigrationMu.Unlock()
	unlock, err := lockHomeMigration(afero.NewOsFs(), home)
	if err != nil {
		return cfg, err
	}
	defer unlock()
	manifest := filepath.Join(cfg.Location, name+".toml")
	if _, err := os.Lstat(manifest); err == nil {
		return cfg, nil
	} else if !os.IsNotExist(err) {
		return cfg, err
	}
	if legacy.Location == "" {
		return cfg, nil
	}
	driver, err := config.GetDriver(legacy, name)
	if err != nil {
		// No readable legacy installation; the caller will install the driver.
		return cfg, nil
	}
	oldLibrary := driver.Driver.Shared.Get(config.PlatformTuple())
	rel, err := filepath.Rel(legacy.Location, oldLibrary)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		// Do not copy arbitrary external libraries referenced by custom manifests.
		return cfg, nil
	}
	if _, err := os.Stat(oldLibrary); os.IsNotExist(err) {
		return cfg, nil
	} else if err != nil {
		return cfg, err
	}
	newLibrary := filepath.Join(cfg.Location, rel)
	// A flat installation shares its directory with driver manifests. Copy
	// libraries and their supporting files, but let each driver publish its
	// own rewritten manifest instead of importing any legacy absolute paths.
	skipManifest := func(path string) bool {
		return filepath.Dir(rel) == "." && filepath.Dir(path) == "." && strings.EqualFold(filepath.Ext(path), ".toml")
	}
	if err := copyHomeFiles(afero.NewOsFs(), filepath.Dir(oldLibrary), filepath.Dir(newLibrary), skipManifest); err != nil {
		return cfg, fmt.Errorf("failed to copy legacy %s ADBC driver: %w", name, err)
	}
	// Clear scalar paths as well as platform tables before rebasing the manifest.
	driver.Driver.Shared = (config.DriverInfo{}).Driver.Shared
	driver.Driver.Shared.Set(config.PlatformTuple(), newLibrary)
	staging, err := os.MkdirTemp(home, ".adbc-migration-*")
	if err != nil {
		return cfg, err
	}
	defer func() { _ = os.RemoveAll(staging) }()
	// dbc also creates a compatibility symlink above Location. Keep both
	// outputs inside staging, then publish the manifest without replacing a
	// newer installation produced by another process during migration.
	stagedConfig := config.Config{Level: config.ConfigEnv, Location: filepath.Join(staging, "drivers")}
	if err := config.CreateManifest(stagedConfig, driver); err != nil {
		return cfg, fmt.Errorf("failed to relocate %s ADBC manifest: %w", name, err)
	}
	return cfg, copyHomeFile(afero.NewOsFs(), filepath.Join(stagedConfig.Location, name+".toml"), manifest, 0o644)
}
