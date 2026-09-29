package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func requireOwnerOnly(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestNewConfigFilesAreOwnerOnly(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not meaningful on Windows")
	}

	tests := map[string]func(fs afero.Fs, path string) error{
		"LoadOrCreate": func(fs afero.Fs, path string) error {
			_, err := LoadOrCreate(fs, path)
			return err
		},
		"LoadOrCreateWithoutPathAbsolutization": func(fs afero.Fs, path string) error {
			_, err := LoadOrCreateWithoutPathAbsolutization(fs, path)
			return err
		},
		"UpsertDefaultTeam": func(fs afero.Fs, path string) error {
			return UpsertDefaultTeam(fs, path, "acme")
		},
	}

	for name, create := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), ".bruin.yml")
			require.NoError(t, create(afero.NewOsFs(), path))
			requireOwnerOnly(t, path)
		})
	}
}
