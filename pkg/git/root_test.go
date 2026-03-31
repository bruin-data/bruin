package git

import (
	"os"
	"os/exec"
	"path/filepath"
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

// setupTempGitRepo creates a temporary git repository with a main branch and an initial commit.
func setupTempGitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	cmds := [][]string{
		{"git", "init", "-b", "main"},
		{"git", "config", "user.name", "test"},
		{"git", "config", "user.email", "test@test.com"},
	}
	for _, args := range cmds {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		require.NoError(t, cmd.Run(), "failed to run: %v", args)
	}

	// Create a file and commit it
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello"), 0o644))
	addCmd := exec.Command("git", "add", "-A")
	addCmd.Dir = dir
	require.NoError(t, addCmd.Run())

	commitCmd := exec.Command("git", "commit", "-m", "initial commit")
	commitCmd.Dir = dir
	require.NoError(t, commitCmd.Run())

	return dir
}

func TestDefaultBranch(t *testing.T) {
	t.Parallel()

	t.Run("detects default branch from temp repo", func(t *testing.T) {
		t.Parallel()
		repoDir := setupTempGitRepo(t)
		branch, err := DefaultBranch(repoDir)
		require.NoError(t, err)
		assert.Equal(t, "main", branch)
	})

	t.Run("returns error for non-repo path", func(t *testing.T) {
		t.Parallel()
		_, err := DefaultBranch("/tmp")
		require.Error(t, err)
	})
}

func TestChangedFilesFromBase(t *testing.T) {
	t.Parallel()

	t.Run("returns changed files on feature branch", func(t *testing.T) {
		t.Parallel()
		repoDir := setupTempGitRepo(t)

		// Create a feature branch and modify a file
		checkoutCmd := exec.Command("git", "checkout", "-b", "feature")
		checkoutCmd.Dir = repoDir
		require.NoError(t, checkoutCmd.Run())

		require.NoError(t, os.WriteFile(filepath.Join(repoDir, "new.txt"), []byte("new"), 0o644))
		addCmd := exec.Command("git", "add", "-A")
		addCmd.Dir = repoDir
		require.NoError(t, addCmd.Run())

		commitCmd := exec.Command("git", "commit", "-m", "add new file")
		commitCmd.Dir = repoDir
		require.NoError(t, commitCmd.Run())

		files, err := ChangedFilesFromBase(repoDir, "main")
		require.NoError(t, err)
		assert.Equal(t, []string{"new.txt"}, files)
	})

	t.Run("returns error for invalid base branch", func(t *testing.T) {
		t.Parallel()
		repoDir := setupTempGitRepo(t)
		_, err := ChangedFilesFromBase(repoDir, "nonexistent-branch-xyz-123")
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
