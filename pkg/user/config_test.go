package user

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigManager_EnsureHomeDirExists(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
