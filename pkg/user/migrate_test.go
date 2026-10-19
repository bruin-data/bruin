//go:build !windows

package user

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyHomeFilesRelocatesAliasedSymlinks(t *testing.T) {
	t.Parallel()
	base, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	physical := filepath.Join(base, "physical")
	alias := filepath.Join(base, "alias")
	otherAlias := filepath.Join(base, "other-alias")
	require.NoError(t, os.MkdirAll(physical, 0o755))
	require.NoError(t, os.Symlink(physical, alias))
	require.NoError(t, os.Symlink(physical, otherAlias))
	source := filepath.Join(alias, ".bruin")
	destination := filepath.Join(alias, "custom home")
	require.NoError(t, os.MkdirAll(filepath.Join(source, "bin"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(source, "bin", "tool"), []byte("binary"), 0o755))
	require.NoError(t, os.MkdirAll(destination, 0o755))
	external := filepath.Join(base, "external")
	require.NoError(t, os.WriteFile(external, []byte("external"), 0o600))
	links := []struct {
		name, target, want, contents string
	}{
		{
			name: "alias", target: filepath.Join(source, "bin", "tool"),
			want: filepath.Join(destination, "bin", "tool"), contents: "binary",
		},
		{
			name: "canonical", target: filepath.Join(physical, ".bruin", "bin", "tool"),
			want: filepath.Join(destination, "bin", "tool"), contents: "binary",
		},
		{
			name: "other-alias", target: filepath.Join(otherAlias, ".bruin", "bin", "tool"),
			want: filepath.Join(destination, "bin", "tool"), contents: "binary",
		},
		{
			name: "chain", target: filepath.Join(source, "alias"),
			want: filepath.Join(destination, "alias"), contents: "binary",
		},
		{
			name: "bin-link", target: filepath.Join(source, "bin"),
			want: filepath.Join(destination, "bin"),
		},
		{
			name: "directory-chain", target: filepath.Join(source, "bin-link", "tool"),
			want: filepath.Join(destination, "bin-link", "tool"), contents: "binary",
		},
		{
			name: "dangling", target: filepath.Join(otherAlias, ".bruin", "pending", "tool"),
			want: filepath.Join(destination, "pending", "tool"),
		},
		{
			name: "external", target: external, want: external, contents: "external",
		},
		{
			name: "relative", target: filepath.Join("bin", "tool"),
			want: filepath.Join("bin", "tool"), contents: "binary",
		},
	}
	for _, link := range links {
		require.NoError(t, os.Symlink(link.target, filepath.Join(source, link.name)))
	}

	require.NoError(t, copyHomeFiles(afero.NewOsFs(), source, destination, nil))
	require.NoError(t, os.RemoveAll(source))
	for _, link := range links {
		migrated := filepath.Join(destination, link.name)
		got, err := os.Readlink(migrated)
		require.NoError(t, err)
		assert.Equal(t, link.want, got, link.name)
		if link.contents != "" {
			contents, err := os.ReadFile(migrated)
			require.NoError(t, err, link.name)
			assert.Equal(t, link.contents, string(contents), link.name)
		}
	}
	assert.DirExists(t, filepath.Join(destination, "bin-link"))
	assert.NoFileExists(t, filepath.Join(destination, "dangling"))
	require.NoError(t, os.MkdirAll(filepath.Join(destination, "pending"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(destination, "pending", "tool"), []byte("new binary"), 0o755))
	contents, err := os.ReadFile(filepath.Join(destination, "dangling"))
	require.NoError(t, err)
	assert.Equal(t, "new binary", string(contents))
}
