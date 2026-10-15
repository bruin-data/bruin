package user

import (
	"fmt"
	"os"
	"os/exec"
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
	// DBC captures its user directory at package initialization. Set the
	// environment before starting the test process, not just before each call.
	if os.Getenv("BRUIN_ADBC_TEST_HOME") == "" {
		home := t.TempDir()
		t.Setenv("BRUIN_ADBC_TEST_HOME", home)
		t.Setenv("HOME", home)
		t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestADBCDriverMigration$", "-test.count=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		return
	}
	rel, err := filepath.Rel(os.Getenv("BRUIN_ADBC_TEST_HOME"), config.ConfigUser.ConfigLocation())
	require.NoError(t, err)
	require.True(t, filepath.IsLocal(rel), "legacy fixtures must stay inside the temporary home")
	for name, directory := range map[string]string{"versioned": "duckdb-1.5.2", "beside manifest": ""} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
			t.Setenv("BRUIN_HOME", filepath.Join(home, "new home"))
			old := config.ConfigUser.ConfigLocation()
			library := filepath.Join(old, directory, "libduckdb.so")
			require.NoError(t, os.MkdirAll(filepath.Dir(library), 0o755))
			require.NoError(t, os.WriteFile(library, []byte("driver"), 0o755))
			// Scalar paths need rebasing just as platform-specific shared paths do.
			manifest := fmt.Sprintf("name = 'duckdb'\nversion = '1.5.2'\n[Driver]\nshared = %q\n", library)
			require.NoError(t, os.WriteFile(filepath.Join(old, "duckdb.toml"), []byte(manifest), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(old, "other.toml"), []byte("another legacy manifest"), 0o600))

			cfg, err := EnsureADBCDriverConfig("duckdb")
			require.NoError(t, err)
			require.Equal(t, filepath.Join(home, "new home", "adbc"), cfg.Location)
			require.Equal(t, config.ConfigEnv, cfg.Level)
			driver, err := config.GetDriver(cfg, "duckdb")
			require.NoError(t, err)
			newLibrary := filepath.Join(cfg.Location, directory, "libduckdb.so")
			assert.Equal(t, newLibrary, driver.Driver.Shared.Get(config.PlatformTuple()))
			assert.Equal(t, "1.5.2", driver.Version.String())
			assert.FileExists(t, library)
			// Other drivers must migrate their own manifests before being usable.
			assert.NoFileExists(t, filepath.Join(cfg.Location, "other.toml"))
			path, err := ADBCDriverPath("duckdb")
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(cfg.Location, "duckdb.toml"), path)

			require.NoError(t, os.RemoveAll(old))
			_, err = EnsureADBCDriverConfig("duckdb")
			require.NoError(t, err)
			driver, err = config.GetDriver(cfg, "duckdb")
			require.NoError(t, err)
			contents, err := os.ReadFile(driver.Driver.Shared.Get(config.PlatformTuple()))
			require.NoError(t, err)
			assert.Equal(t, "driver", string(contents))

			t.Setenv("BRUIN_HOME", "")
			path, err = ADBCDriverPath("duckdb")
			require.NoError(t, err)
			assert.Equal(t, "duckdb", path)
		})
	}
}
