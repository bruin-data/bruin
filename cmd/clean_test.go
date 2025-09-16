package cmd

import (
	"errors"
	"fmt"
	"path"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock ConfigManager ---
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

// --- Mock filesystem that fails on Remove ---
type mockFailingFs struct {
	afero.Fs
	removeErr error
}

func (m *mockFailingFs) Remove(name string) error {
	return m.removeErr
}

// --- Mock printer to capture output ---
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
	tests := []struct {
		name           string
		inputPath      string
		cleanUvCache   bool
		setupMocks     func(t *testing.T) (afero.Fs, *mockConfigManager, string)
		expectedErr    string
		expectedOutput []string
	}{
		{
			name:      "config manager error - failed to get bruin home dir",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, string) {
				return afero.NewMemMapFs(), &mockConfigManager{
					homeDirErr: errors.New("failed to get bruin home directory"),
				}, "."
			},
			expectedErr: "failed to get bruin home directory",
		},
		{
			name:      "config manager error - failed to recreate home dir",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, string) {
				fs := afero.NewMemMapFs()
				repoRoot := "/test-repo"
				fs.MkdirAll(path.Join(repoRoot, LogsFolder), 0755)
				return fs, &mockConfigManager{
					bruinHomeDir: "/test-bruin-home",
					recreateErr:  errors.New("failed to recreate the home directory"),
				}, repoRoot
			},
			expectedErr: "failed to recreate the home directory",
		},
		{
			name:      "git repo not found",
			inputPath: "/nonexistent-path",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, string) {
				return afero.NewMemMapFs(), &mockConfigManager{
					bruinHomeDir: "/test-bruin-home",
				}, "/nonexistent-path"
			},
			expectedErr: "Failed to find the git repository root",
		},
		{
			name:      "no log files found",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, string) {
				fs := afero.NewMemMapFs()
				repoRoot := "/test-repo"
				fs.MkdirAll(path.Join(repoRoot, LogsFolder), 0755)
				return fs, &mockConfigManager{
					bruinHomeDir: "/test-bruin-home",
				}, repoRoot
			},
			expectedOutput: []string{"No log files found, nothing to clean up..."},
		},
		{
			name:      "successful cleanup with log files",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, string) {
				fs := afero.NewMemMapFs()
				repoRoot := "/test-repo"
				logsFolder := path.Join(repoRoot, LogsFolder)
				fs.MkdirAll(logsFolder, 0755)
				logFiles := []string{"test1.log", "test2.log", "test3.log"}
				for _, f := range logFiles {
					file, _ := fs.Create(path.Join(logsFolder, f))
					file.WriteString("test log content")
					file.Close()
				}
				return fs, &mockConfigManager{
					bruinHomeDir: "/test-bruin-home",
				}, repoRoot
			},
			expectedOutput: []string{
				"Found 3 log files, cleaning them up...",
				"Successfully removed 3 log files.",
			},
		},
		{
			name:      "file removal error",
			inputPath: ".",
			setupMocks: func(t *testing.T) (afero.Fs, *mockConfigManager, string) {
				fs := &mockFailingFs{
					Fs:        afero.NewMemMapFs(),
					removeErr: errors.New("permission denied"),
				}
				repoRoot := "/test-repo"
				logsFolder := path.Join(repoRoot, LogsFolder)
				fs.MkdirAll(logsFolder, 0755)
				file, _ := fs.Create(path.Join(logsFolder, "test1.log"))
				file.WriteString("test log content")
				file.Close()
				return fs, &mockConfigManager{
					bruinHomeDir: "/test-bruin-home",
				}, repoRoot
			},
			expectedErr: "failed to remove file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, mockCM, repoRoot := tt.setupMocks(t)

			output := []string{}
			mockPrinter := &mockOutputPrinter{output: &output}
			cmd := NewCleanCommand(mockCM, fs, mockPrinter, mockPrinter)

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
