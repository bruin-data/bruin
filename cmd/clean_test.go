package cmd

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testRepoRoot = "/test-repo"

// --- Mock ConfigManager ---.
type mockConfigManager struct {
	bruinHomeDir string
	homeDirErr   error
	recreateErr  error
}

func (m *mockConfigManager) EnsureAndGetBruinHomeDir() (string, error) {
	return m.bruinHomeDir, m.homeDirErr
}

func (m *mockConfigManager) RecreateHomeDir() error {
	return m.recreateErr
}

// --- Mock GitFinder ---.
type mockGitFinder struct {
	repo *git.Repo
	err  error
}

func (m *mockGitFinder) Repo(path string) (*git.Repo, error) {
	return m.repo, m.err
}

// --- Mock filesystem that fails on Remove ---.
type mockFailingFs struct {
	afero.Fs
	removeErr error
}

func (m *mockFailingFs) Remove(name string) error {
	return m.removeErr
}

// --- Mock printer to capture output ---.
type mockOutputPrinter struct {
	output *[]string
}

func (m *mockOutputPrinter) Printf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	*m.output = append(*m.output, msg)
}

func (m *mockOutputPrinter) Println(args ...interface{}) {
	msg := fmt.Sprint(args...)
	*m.output = append(*m.output, msg)
}

func TestCleanCommand_Run(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		inputPath      string
		cleanUvCache   bool
		setupMocks     func(t *testing.T) (afero.Fs, *mockConfigManager, *mockGitFinder, string)
		expectedErr    string
		expectedOutput []string
	}{
		{
			name:      "config manager error - failed to get bruin home dir",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, *mockGitFinder, string) {
				return afero.NewMemMapFs(), &mockConfigManager{
					homeDirErr: errors.New("failed to get bruin home directory"),
				}, &mockGitFinder{}, "."
			},
			expectedErr: "failed to get bruin home directory",
		},
		{
			name:      "config manager error - failed to recreate home dir",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, *mockGitFinder, string) {
				fs := afero.NewMemMapFs()
				repoRoot := testRepoRoot
				require.NoError(t, fs.MkdirAll(path.Join(repoRoot, LogsFolder), 0o755))
				return fs, &mockConfigManager{
					bruinHomeDir: "/test-bruin-home",
					recreateErr:  errors.New("failed to recreate the home directory"),
				}, &mockGitFinder{}, repoRoot
			},
			expectedErr: "failed to recreate the home directory",
		},
		{
			name:      "git repo not found",
			inputPath: "/nonexistent-path",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, *mockGitFinder, string) {
				return afero.NewMemMapFs(), &mockConfigManager{
						bruinHomeDir: "/test-bruin-home",
					}, &mockGitFinder{
						err: errors.New("no git repository found"),
					}, "/nonexistent-path"
			},
			expectedErr: "failed to find the git repository root",
		},
		{
			name:      "no log files found",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, *mockGitFinder, string) {
				fs := afero.NewMemMapFs()
				repoRoot := testRepoRoot
				require.NoError(t, fs.MkdirAll(path.Join(repoRoot, LogsFolder), 0o755))
				return fs, &mockConfigManager{
						bruinHomeDir: "/test-bruin-home",
					}, &mockGitFinder{
						repo: &git.Repo{Path: repoRoot},
					}, repoRoot
			},
			expectedOutput: []string{"No log files found, nothing to clean up..."},
		},
		{
			name:      "successful cleanup with log files",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, *mockGitFinder, string) {
				fs := afero.NewMemMapFs()
				repoRoot := testRepoRoot
				logsFolder := path.Join(repoRoot, LogsFolder)
				require.NoError(t, fs.MkdirAll(logsFolder, 0o755))
				logFiles := []string{"test1.log", "test2.log", "test3.log"}
				for _, f := range logFiles {
					file, _ := fs.Create(path.Join(logsFolder, f))
					_, err := file.WriteString("test log content")
					require.NoError(t, err)
					file.Close()
				}
				return fs, &mockConfigManager{
						bruinHomeDir: "/test-bruin-home",
					}, &mockGitFinder{
						repo: &git.Repo{Path: repoRoot},
					}, repoRoot
			},
			expectedOutput: []string{
				"Found 3 log files, cleaning them up...\n",
				"Successfully removed 3 log files.\n",
			},
		},
		{
			name:      "file removal error",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, *mockGitFinder, string) {
				fs := &mockFailingFs{
					Fs:        afero.NewMemMapFs(),
					removeErr: errors.New("permission denied"),
				}
				repoRoot := testRepoRoot
				logsFolder := path.Join(repoRoot, LogsFolder)
				require.NoError(t, fs.MkdirAll(logsFolder, 0o755))
				file, _ := fs.Create(path.Join(logsFolder, "test1.log"))
				_, err := file.WriteString("test log content")
				require.NoError(t, err)
				file.Close()
				return fs, &mockConfigManager{
						bruinHomeDir: "/test-bruin-home",
					}, &mockGitFinder{
						repo: &git.Repo{Path: repoRoot},
					}, repoRoot
			},
			expectedErr: "failed to remove file",
		},
		{
			name:         "successful cleanup with uv cache",
			inputPath:    ".",
			cleanUvCache: true,
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, *mockGitFinder, string) {
				fs := afero.NewMemMapFs()
				repoRoot := testRepoRoot
				require.NoError(t, fs.MkdirAll(path.Join(repoRoot, LogsFolder), 0o755))
				return fs, &mockConfigManager{
						bruinHomeDir: "/test-bruin-home",
					}, &mockGitFinder{
						repo: &git.Repo{Path: repoRoot},
					}, repoRoot
			},
			expectedOutput: []string{"No log files found, nothing to clean up..."},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs, mockCM, mockGF, repoRoot := tt.setupMocks(t)

			output := []string{}
			mockPrinter := &mockOutputPrinter{output: &output}
			cmd := NewCleanCommand(mockCM, mockGF, fs, mockPrinter, mockPrinter)

			err := cmd.Run(repoRoot, tt.cleanUvCache)

			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
				for _, expected := range tt.expectedOutput {
					assert.Contains(t, output, expected)
				}
			}
		})
	}
}

func TestCleanCommand_cleanUvCache(t *testing.T) {
	t.Parallel()

	// Test case: uv binary does not exist
	t.Run("uv binary does not exist", func(t *testing.T) {
		t.Parallel()

		// Create a temporary directory that doesn't contain uv
		tempDir := t.TempDir()
		output := []string{}
		printer := &mockOutputPrinter{output: &output}

		cmd := &CleanCommand{
			infoPrinter:  printer,
			errorPrinter: printer,
		}

		err := cmd.cleanUvCache(tempDir)

		require.NoError(t, err)
		assert.Contains(t, *printer.output, "UV is not installed yet. Nothing to clean.")
	})
}

func TestCleanCommand_confirmUvCacheClean(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		input          string
		expectedResult bool
	}{
		{"user confirms with 'y'", "y\n", true},
		{"user confirms with 'yes'", "yes\n", true},
		{"user confirms with 'Y'", "Y\n", true},
		{"user cancels with 'n'", "n\n", false},
		{"user cancels with 'no'", "no\n", false},
		{"user cancels with empty input", "\n", false},
		{"user cancels with random text", "maybe\n", false},
		{"user cancels with whitespace", "  n  \n", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			output := []string{}
			cmd := &CleanCommand{
				errorPrinter: &mockOutputPrinter{output: &output},
			}

			reader := strings.NewReader(tt.input)
			result := cmd.confirmUvCacheClean(reader)

			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestCleanCommand_confirmUvCacheClean_Error(t *testing.T) {
	t.Parallel()

	output := []string{}
	cmd := &CleanCommand{
		errorPrinter: &mockOutputPrinter{output: &output},
	}

	// Broken reader that always errors
	brokenReader := iotest.ErrReader(errors.New("simulated read error"))

	result := cmd.confirmUvCacheClean(brokenReader)

	assert.False(t, result)
	assert.Len(t, output, 1)
	assert.Contains(t, output[0], "Error reading input")
}
