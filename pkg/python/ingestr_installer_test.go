package python

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestrCheckerInstallsAndCachesExactRelease(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

func TestIngestrInstallerRequiresShellOnUnix(t *testing.T) {
	t.Parallel()

	installer := ingestrInstallerRuntime{
		goos: "linux",
		findShell: func(string) (string, error) {
			return "", os.ErrNotExist
		},
	}

	err := installer.install(t.Context(), io.Discard, t.TempDir(), "1.2.3")

	require.ErrorContains(t, err, "requires sh")
}

func TestIngestrInstallerFallsBackToNativeWindowsDownload(t *testing.T) {
	t.Parallel()

	archive := windowsIngestrTestArchive(t, "ingestr.exe", "windows binary")
	requestedPath := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestedPath <- request.URL.Path
		writer.Header().Set("Content-Type", "application/zip")
		_, _ = writer.Write(archive)
	}))
	t.Cleanup(server.Close)
	installDir := t.TempDir()
	installer := ingestrInstallerRuntime{
		goos:   "windows",
		goarch: "amd64",
		findShell: func(string) (string, error) {
			return "", os.ErrNotExist
		},
		httpClient:         server.Client(),
		releaseDownloadURL: server.URL,
	}

	err := installer.install(t.Context(), io.Discard, installDir, "1.2.3")

	require.NoError(t, err)
	assert.Equal(t, "/v1.2.3/ingestr_Windows_x86_64.zip", <-requestedPath)
	contents, err := os.ReadFile(filepath.Join(installDir, "ingestr.exe"))
	require.NoError(t, err)
	assert.Equal(t, "windows binary", string(contents))
}

func TestNativeWindowsIngestrInstallerRejectsFailedDownload(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "not found", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	installer := ingestrInstallerRuntime{
		goarch:             "amd64",
		httpClient:         server.Client(),
		releaseDownloadURL: server.URL,
	}

	err := installer.installWindowsRelease(t.Context(), io.Discard, t.TempDir(), "1.2.3")

	require.ErrorContains(t, err, "server returned 404 Not Found")
}

func TestNativeWindowsIngestrInstallerRequiresBinaryInArchive(t *testing.T) {
	t.Parallel()

	archive := windowsIngestrTestArchive(t, "README.md", "missing binary")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write(archive)
	}))
	t.Cleanup(server.Close)
	installer := ingestrInstallerRuntime{
		goarch:             "amd64",
		httpClient:         server.Client(),
		releaseDownloadURL: server.URL,
	}

	err := installer.installWindowsRelease(t.Context(), io.Discard, t.TempDir(), "1.2.3")

	require.ErrorContains(t, err, "archive did not contain ingestr.exe")
}

func TestWindowsIngestrArchiveNameRejectsUnsupportedArchitecture(t *testing.T) {
	t.Parallel()

	_, err := windowsIngestrArchiveName("arm64")

	require.ErrorContains(t, err, "do not support windows/arm64")
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

func windowsIngestrTestArchive(t *testing.T, name, contents string) []byte {
	t.Helper()

	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	file, err := archive.Create(name)
	require.NoError(t, err)
	_, err = file.Write([]byte(contents))
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	return buffer.Bytes()
}
