package path

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestWriteSecretFileCreatesOwnerOnlyFile(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not meaningful on Windows")
	}

	path := filepath.Join(t.TempDir(), ".bruin.yml")
	require.NoError(t, WriteSecretFile(afero.NewOsFs(), path, []byte("secret\n")))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestWriteSecretFileKeepsExistingMode(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not meaningful on Windows")
	}

	path := filepath.Join(t.TempDir(), ".bruin.yml")
	require.NoError(t, os.WriteFile(path, []byte("old\n"), 0o640))
	require.NoError(t, os.Chmod(path, 0o640))

	require.NoError(t, WriteSecretFile(afero.NewOsFs(), path, []byte("new\n")))

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "new\n", string(content))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
}

func TestWriteSecretYamlCreatesOwnerOnlyFile(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not meaningful on Windows")
	}

	path := filepath.Join(t.TempDir(), ".bruin.yml")
	require.NoError(t, WriteSecretYaml(afero.NewOsFs(), path, map[string]string{"password": "hunter2"}))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestWriteYamlKeepsCommittableMode(t *testing.T) {
	t.Parallel()

	// MemMapFs records the requested mode without applying the process umask.
	fs := afero.NewMemMapFs()
	require.NoError(t, WriteYaml(fs, "pipeline.yml", map[string]string{"name": "p"}))

	info, err := fs.Stat("pipeline.yml")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
}
