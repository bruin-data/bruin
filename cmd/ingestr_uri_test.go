package cmd

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteSecretFile(t *testing.T) {
	t.Parallel()

	const uri = "postgresql://u:p@h:5432/db"

	t.Run("writes the bare uri with no trailing newline", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "uri.txt")
		require.NoError(t, writeSecretFile(path, uri))

		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, uri, string(contents))
	})

	t.Run("creates the file owner readable only", func(t *testing.T) {
		t.Parallel()

		if runtime.GOOS == "windows" {
			t.Skip("unix permission bits do not map onto windows ACLs")
		}

		path := filepath.Join(t.TempDir(), "uri.txt")
		require.NoError(t, writeSecretFile(path, uri))

		info, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	})

	t.Run("refuses to overwrite an existing path", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "uri.txt")
		require.NoError(t, os.WriteFile(path, []byte("pre-existing"), 0o600))

		err := writeSecretFile(path, uri)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create the output file")

		// The original contents must survive, so nothing leaks into a file the
		// caller did not create.
		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, "pre-existing", string(contents))
	})

	t.Run("does not leave a file behind when the directory is missing", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "missing", "uri.txt")

		err := writeSecretFile(path, uri)
		require.Error(t, err)
		assert.NoFileExists(t, path)
	})
}
