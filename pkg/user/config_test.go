package user

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestConfigManager_EnsureHomeDirExists(t *testing.T) {
	t.Parallel()

	homeDir, err := os.UserHomeDir()
	assert.NoError(t, err)
	assert.NotEmpty(t, homeDir)

	fs := afero.NewMemMapFs()

	c := &ConfigManager{fs: fs}

	err = c.EnsureHomeDirExists()
	assert.NoError(t, err)
	assert.Equal(t, homeDir, c.userHomeDir)
	assert.Equal(t, filepath.Join(homeDir, bruinHomeDir), c.bruinHomeDir)

	fileInfo, err := fs.Stat(c.bruinHomeDir)
	assert.NoError(t, err)
	assert.True(t, fileInfo.IsDir())

	// ensure repetitive calls are safe
	err = c.EnsureHomeDirExists()
	assert.NoError(t, err)
}

func TestConfigManager_EnsureVirtualenvDirExists(t *testing.T) {
	t.Parallel()

	homeDir, err := os.UserHomeDir()
	assert.NoError(t, err)
	assert.NotEmpty(t, homeDir)

	fs := afero.NewMemMapFs()
	c := &ConfigManager{fs: fs}

	err = c.EnsureVirtualenvDirExists()
	assert.NoError(t, err)
	assert.Equal(t, filepath.Join(homeDir, bruinHomeDir), c.bruinHomeDir)

	fileInfo, err := fs.Stat(filepath.Join(c.bruinHomeDir, virtualEnvsPath))
	assert.NoError(t, err)
	assert.True(t, fileInfo.IsDir())

	// ensure repetitive calls are safe
	err = c.EnsureVirtualenvDirExists()
	assert.NoError(t, err)
}
