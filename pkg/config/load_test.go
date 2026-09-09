package config

import (
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestLoadMissingConfig(t *testing.T) {
	t.Parallel()

	filesystem := afero.NewMemMapFs()
	configPath := "/project/missing.yml"
	cfg, err := Load(filesystem, configPath)
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.ErrorContains(t, err, configPath)
	require.Nil(t, cfg)

	for _, path := range []string{configPath, filepath.Join(filepath.Dir(configPath), ".gitignore")} {
		exists, err := afero.Exists(filesystem, path)
		require.NoError(t, err)
		require.False(t, exists)
	}
}

func TestLoadExistingConfig(t *testing.T) {
	t.Parallel()

	filesystem := afero.NewMemMapFs()
	configPath := "/project/config.yml"
	content := []byte("default_environment: dev\nenvironments:\n  dev:\n    connections: {}\n")
	require.NoError(t, afero.WriteFile(filesystem, configPath, content, 0o600))

	cfg, err := Load(filesystem, configPath)
	require.NoError(t, err)
	require.Equal(t, "dev", cfg.SelectedEnvironmentName)
	actual, err := afero.ReadFile(filesystem, configPath)
	require.NoError(t, err)
	require.Equal(t, content, actual)
}
