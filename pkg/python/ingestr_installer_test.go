package python

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestrCheckerInstallsAndCachesExactRelease(t *testing.T) {
	homeDir := t.TempDir()
	installCalls := 0
	checker := &IngestrChecker{
		install: func(_ context.Context, _ io.Writer, installDir, version string) error {
			installCalls++
			assert.Equal(t, "1.2.3", version)
			return os.WriteFile(
				filepath.Join(installDir, ingestrBinaryName(runtime.GOOS)),
				[]byte("installed "+version),
				0o755,
			)
		},
	}

	binaryPath, err := checker.ensureIngestrInstalled(t.Context(), homeDir, "1.2.3", io.Discard)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(homeDir, "ingestr", "1.2.3", ingestrBinaryName(runtime.GOOS)), binaryPath)
	assert.Equal(t, 1, installCalls)

	contents, err := os.ReadFile(binaryPath)
	require.NoError(t, err)
	assert.Equal(t, "installed 1.2.3", string(contents))

	// A cached release is returned without invoking the installer again.
	cachedPath, err := checker.ensureIngestrInstalled(t.Context(), homeDir, "1.2.3", io.Discard)
	require.NoError(t, err)
	assert.Equal(t, binaryPath, cachedPath)
	assert.Equal(t, 1, installCalls)
	assert.Empty(t, installationTempDirs(t, homeDir))
}

func TestIngestrCheckerCleansUpFailedInstallation(t *testing.T) {
	homeDir := t.TempDir()
	installErr := assert.AnError
	checker := &IngestrChecker{
		install: func(context.Context, io.Writer, string, string) error {
			return installErr
		},
	}

	_, err := checker.ensureIngestrInstalled(t.Context(), homeDir, "1.2.3", io.Discard)

	require.ErrorIs(t, err, installErr)
	assert.NoFileExists(t, filepath.Join(homeDir, "ingestr", "1.2.3", ingestrBinaryName(runtime.GOOS)))
	assert.Empty(t, installationTempDirs(t, homeDir))
}

func TestIngestrCheckerRejectsInstallerWithoutBinary(t *testing.T) {
	homeDir := t.TempDir()
	checker := &IngestrChecker{
		install: func(context.Context, io.Writer, string, string) error {
			return nil
		},
	}

	_, err := checker.ensureIngestrInstalled(t.Context(), homeDir, "1.2.3", io.Discard)

	require.ErrorContains(t, err, "installer did not produce "+ingestrBinaryName(runtime.GOOS))
	assert.Empty(t, installationTempDirs(t, homeDir))
}

func TestIngestrCheckerRejectsInvalidVersion(t *testing.T) {
	checker := &IngestrChecker{}

	_, err := checker.EnsureIngestrInstalled(t.Context(), "../../unexpected")

	require.ErrorContains(t, err, "invalid ingestr version")
}

func TestIngestrInstallerCommandArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		goos       string
		installDir string
		wantDir    string
	}{
		{
			name:       "linux path",
			goos:       "linux",
			installDir: "/tmp/bruin home/ingestr",
			wantDir:    "/tmp/bruin home/ingestr",
		},
		{
			name:       "windows path is converted for msys",
			goos:       "windows",
			installDir: `C:\Users\runner admin\.bruin\ingestr`,
			wantDir:    "C:/Users/runner admin/.bruin/ingestr",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			args := ingestrInstallerCommandArgs(tt.installDir, "1.2.3", tt.goos)

			assert.Equal(t, []string{
				"-c",
				ingestrInstallerShellCommand,
				"ingestr-installer",
				ingestrInstallerURL,
				tt.wantDir,
				"v1.2.3",
			}, args)
		})
	}
}

func TestIngestrBinaryName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ingestr", ingestrBinaryName("linux"))
	assert.Equal(t, "ingestr", ingestrBinaryName("darwin"))
	assert.Equal(t, "ingestr.exe", ingestrBinaryName("windows"))
}

func TestRunIngestrInstallerRequiresShell(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	err := runIngestrInstaller(t.Context(), io.Discard, t.TempDir(), "1.2.3")

	require.ErrorContains(t, err, "requires sh")
}

func TestEnvironmentWithOverride(t *testing.T) {
	t.Parallel()

	result := environmentWithOverride([]string{"PATH=/bin", "SHELL=/bin/zsh", "VALUE=a=b"}, "SHELL", "bruin-installer")

	assert.ElementsMatch(t, []string{"PATH=/bin", "VALUE=a=b", "SHELL=bruin-installer"}, result)
}

func installationTempDirs(t *testing.T, homeDir string) []string {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(homeDir, "ingestr", ".install-*"))
	require.NoError(t, err)
	return matches
}
