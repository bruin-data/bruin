package user

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/columnar-tech/dbc/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestADBCDriverMigration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("legacy user manifests are stored in the real Windows registry")
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	t.Setenv("BRUIN_HOME", filepath.Join(home, "new home"))
	old := config.ConfigUser.ConfigLocation()
	library := filepath.Join(old, "duckdb-1.5.2", "libduckdb.so")
	require.NoError(t, os.MkdirAll(filepath.Dir(library), 0o755))
	require.NoError(t, os.WriteFile(library, []byte("driver"), 0o755))
	// Scalar paths need rebasing just as platform-specific shared paths do.
	manifest := fmt.Sprintf("name = 'duckdb'\nversion = '1.5.2'\n[Driver]\nshared = %q\n", library)
	require.NoError(t, os.WriteFile(filepath.Join(old, "duckdb.toml"), []byte(manifest), 0o600))

	cfg, err := EnsureADBCDriverConfig("duckdb")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(home, "new home", "adbc"), cfg.Location)
	require.Equal(t, config.ConfigEnv, cfg.Level)
	driver, err := config.GetDriver(cfg, "duckdb")
	require.NoError(t, err)
	newLibrary := filepath.Join(cfg.Location, "duckdb-1.5.2", "libduckdb.so")
	assert.Equal(t, newLibrary, driver.Driver.Shared.Get(config.PlatformTuple()))
	assert.Equal(t, "1.5.2", driver.Version.String())
	assert.FileExists(t, library)
	path, err := ADBCDriverPath("duckdb")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(cfg.Location, "duckdb.toml"), path)

	require.NoError(t, os.RemoveAll(old))
	_, err = EnsureADBCDriverConfig("duckdb")
	require.NoError(t, err)
	contents, err := os.ReadFile(newLibrary)
	require.NoError(t, err)
	assert.Equal(t, "driver", string(contents))

	t.Setenv("BRUIN_HOME", "")
	path, err = ADBCDriverPath("duckdb")
	require.NoError(t, err)
	assert.Equal(t, "duckdb", path)
}
