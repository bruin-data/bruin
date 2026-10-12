package user

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigManager_EnsureHomeDirExists(t *testing.T) {
	t.Setenv("BRUIN_HOME", "")

	homeDir, err := os.UserHomeDir()
	require.NoError(t, err)
	assert.NotEmpty(t, homeDir)

	fs := afero.NewMemMapFs()

	c := &ConfigManager{fs: fs}

	err = c.EnsureHomeDirExists()
	require.NoError(t, err)
	assert.Equal(t, homeDir, c.userHomeDir)
	assert.Equal(t, filepath.Join(homeDir, bruinHomeDir), c.bruinHomeDir)

	fileInfo, err := fs.Stat(c.bruinHomeDir)
	require.NoError(t, err)
	assert.True(t, fileInfo.IsDir())

	// ensure repetitive calls are safe
	err = c.EnsureHomeDirExists()
	assert.NoError(t, err)
}

func TestConfigManager_EnsureVirtualenvDirExists(t *testing.T) {
	t.Setenv("BRUIN_HOME", "")

	homeDir, err := os.UserHomeDir()
	require.NoError(t, err)
	assert.NotEmpty(t, homeDir)

	fs := afero.NewMemMapFs()
	c := &ConfigManager{fs: fs}

	err = c.EnsureVirtualenvDirExists()
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(homeDir, bruinHomeDir), c.bruinHomeDir)

	fileInfo, err := fs.Stat(filepath.Join(c.bruinHomeDir, virtualEnvsPath))
	require.NoError(t, err)
	assert.True(t, fileInfo.IsDir())

	// ensure repetitive calls are safe
	err = c.EnsureVirtualenvDirExists()
	require.NoError(t, err)
}

func TestBruinHomeMigration(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	old := filepath.Join(home, ".bruin")
	destination := filepath.Join(t.TempDir(), "custom home")
	t.Setenv("BRUIN_HOME", destination)
	require.NoError(t, os.MkdirAll(filepath.Join(old, "ingestr", "v1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(old, "ingestr", "v1", "ingestr"), []byte("binary"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(old, "telemetry.json"), []byte("old"), 0o600))
	require.NoError(t, os.MkdirAll(destination, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(destination, "telemetry.json"), []byte("new"), 0o600))
	if runtime.GOOS != "windows" {
		require.NoError(t, os.Symlink(filepath.Join(old, "ingestr", "v1", "ingestr"), filepath.Join(old, "tool")))
		readOnlyDir := filepath.Join(old, "ingestr", "v1")
		require.NoError(t, os.Chmod(readOnlyDir, 0o555))
		t.Cleanup(func() { _ = os.Chmod(readOnlyDir, 0o755) })
	}

	cm := NewConfigManager(nil)
	got, err := cm.EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	require.Equal(t, destination, got)
	contents, err := os.ReadFile(filepath.Join(destination, "ingestr", "v1", "ingestr"))
	require.NoError(t, err)
	assert.Equal(t, "binary", string(contents))
	contents, err = os.ReadFile(filepath.Join(destination, "telemetry.json"))
	require.NoError(t, err)
	assert.Equal(t, "new", string(contents))
	assert.FileExists(t, filepath.Join(old, "ingestr", "v1", "ingestr"))
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(destination, "ingestr", "v1", "ingestr"))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o755), info.Mode().Perm())
		link, err := os.Readlink(filepath.Join(destination, "tool"))
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(destination, "ingestr", "v1", "ingestr"), link)
	}
	require.NoError(t, cm.EnsureVirtualenvDirExists())
	assert.DirExists(t, cm.MakeVirtualenvPath(""))
	require.NoError(t, cm.RecreateHomeDir())
	_, err = NewConfigManager(nil).EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	assert.NoFileExists(t, filepath.Join(destination, "ingestr", "v1", "ingestr"))
	assert.FileExists(t, filepath.Join(old, "ingestr", "v1", "ingestr"))
}

func TestBruinHomeWithoutUserHome(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("USERPROFILE", "")
	destination := filepath.Join(t.TempDir(), "bruin")
	t.Setenv("BRUIN_HOME", destination)
	got, err := NewConfigManager(nil).EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	assert.Equal(t, destination, got)
	assert.DirExists(t, got)
}

func TestBruinHomeInsideLegacyHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	old := filepath.Join(home, ".bruin")
	destination := filepath.Join(old, "new")
	t.Setenv("BRUIN_HOME", destination)
	require.NoError(t, os.MkdirAll(old, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(old, "uv"), []byte("uv"), 0o755))
	_, err := NewConfigManager(nil).EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	assert.FileExists(t, filepath.Join(destination, "uv"))
	assert.NoDirExists(t, filepath.Join(destination, "new"))
}
