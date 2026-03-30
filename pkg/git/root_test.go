package git

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		want    *Repo
		wantErr bool
	}{
		{
			name:    "no repo exists",
			path:    "/",
			wantErr: true,
		},
		{
			name: "can find its own repo root",
			path: ".",
			want: &Repo{
				Path: path.AbsPathForTests(t, "../../."),
			},
		},
		{
			name: "can find its own repo root even if a file is given",
			path: "./root_test.go",
			want: &Repo{
				Path: path.AbsPathForTests(t, "../../."),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := FindRepoFromPath(tt.path)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDefaultBranch(t *testing.T) {
	t.Parallel()

	t.Run("detects default branch from current repo", func(t *testing.T) {
		t.Parallel()
		repoRoot := path.AbsPathForTests(t, "../../.")
		branch, err := DefaultBranch(repoRoot)
		require.NoError(t, err)
		assert.Contains(t, []string{"main", "master"}, branch)
	})

	t.Run("returns error for non-repo path", func(t *testing.T) {
		t.Parallel()
		_, err := DefaultBranch("/tmp")
		require.Error(t, err)
	})
}

func TestChangedFilesFromBase(t *testing.T) {
	t.Parallel()

	t.Run("returns no error for current repo", func(t *testing.T) {
		t.Parallel()
		repoRoot := path.AbsPathForTests(t, "../../.")
		branch, err := DefaultBranch(repoRoot)
		require.NoError(t, err)

		files, err := ChangedFilesFromBase(repoRoot, branch)
		require.NoError(t, err)
		// Files may be empty or non-empty depending on branch state, just check no error
		_ = files
	})

	t.Run("returns error for invalid base branch", func(t *testing.T) {
		t.Parallel()
		repoRoot := path.AbsPathForTests(t, "../../.")
		_, err := ChangedFilesFromBase(repoRoot, "nonexistent-branch-xyz-123")
		require.Error(t, err)
	})
}

func BenchmarkFindRepoFromPath(b *testing.B) {
	// Reset the timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		_, err := FindRepoFromPath(".")
		if err != nil {
			b.Fatal(err)
		}
	}
}
