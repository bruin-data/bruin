package git

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureGivenPatternIsInGitignore(t *testing.T) {
	t.Parallel()

	t.Run("already-present pattern needs no write access", func(t *testing.T) {
		t.Parallel()
		base := afero.NewMemMapFs()
		require.NoError(t, afero.WriteFile(base, "/repo/.gitignore", []byte("foo\nlogs/queries\nbar"), 0o644))
		fs := afero.NewReadOnlyFs(base)

		assert.NoError(t, EnsureGivenPatternIsInGitignore(fs, "/repo", "logs/queries"))
	})

	t.Run("appends a missing pattern", func(t *testing.T) {
		t.Parallel()
		fs := afero.NewMemMapFs()
		require.NoError(t, afero.WriteFile(fs, "/repo/.gitignore", []byte("foo"), 0o644))

		require.NoError(t, EnsureGivenPatternIsInGitignore(fs, "/repo", "logs/queries"))

		content, err := afero.ReadFile(fs, "/repo/.gitignore")
		require.NoError(t, err)
		assert.Contains(t, string(content), "logs/queries")
	})

	t.Run("creates .gitignore when absent", func(t *testing.T) {
		t.Parallel()
		fs := afero.NewMemMapFs()

		require.NoError(t, EnsureGivenPatternIsInGitignore(fs, "/repo", "logs/queries"))

		content, err := afero.ReadFile(fs, "/repo/.gitignore")
		require.NoError(t, err)
		assert.Contains(t, string(content), "logs/queries")
	})
}
