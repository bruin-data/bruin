package python

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestrCheckerInstallsAndCachesExactRelease(t *testing.T) {
	t.Parallel()
	home := t.TempDir()
	calls := 0
	checker := &IngestrChecker{install: func(_ context.Context, _ io.Writer, dir, version string) error {
		calls++
		assert.Equal(t, "1.2.3", version)
		return os.WriteFile(filepath.Join(dir, ingestrBinaryName(runtime.GOOS)), []byte("binary"), 0o755)
	}}
	path, err := checker.ensureIngestrInstalled(t.Context(), home, "1.2.3", io.Discard)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(home, "ingestr", "1.2.3", ingestrBinaryName(runtime.GOOS)), path)
	// Unknown cached versions remain usable even without an embedded hash.
	checker.install = nil
	cached, err := checker.ensureIngestrInstalled(t.Context(), home, "1.2.3", io.Discard)
	require.NoError(t, err)
	assert.Equal(t, path, cached)
	assert.Equal(t, 1, calls)
}

func TestIngestrCheckerFailedInstallation(t *testing.T) {
	t.Parallel()
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			home := t.TempDir()
			checker := &IngestrChecker{install: func(context.Context, io.Writer, string, string) error {
				if missing {
					return nil
				}
				return assert.AnError
			}}
			_, err := checker.ensureIngestrInstalled(t.Context(), home, "1.2.3", io.Discard)
			require.Error(t, err)
			assert.NoFileExists(t, filepath.Join(home, "ingestr", "1.2.3", ingestrBinaryName(runtime.GOOS)))
			matches, err := filepath.Glob(filepath.Join(home, "ingestr", ".install-*"))
			require.NoError(t, err)
			assert.Empty(t, matches)
		})
	}
}

func TestIngestrCheckerRejectsInvalidVersion(t *testing.T) {
	t.Parallel()
	_, err := (&IngestrChecker{}).EnsureIngestrInstalled(t.Context(), "../../unexpected")
	require.ErrorContains(t, err, "invalid ingestr version")
}

func TestIngestrFreshDownload(t *testing.T) {
	t.Parallel()
	for _, platform := range []struct{ os, arch, archive string }{
		{"windows", "amd64", "ingestr_Windows_x86_64.zip"},
	} {
		t.Run(platform.os+platform.arch, func(t *testing.T) {
			t.Parallel()
			for _, scenario := range []string{"valid", "tampered", "wrong-version", "unknown", "http-error", "missing-binary", "cancelled"} {
				t.Run(scenario, func(t *testing.T) {
					name := ingestrBinaryName(platform.os)
					entry := "nested/" + name
					if scenario == "missing-binary" {
						entry = "README.md"
					}
					archive := ingestrTestArchive(t, platform.os, entry, "trusted binary")
					digest := fmt.Sprintf("%x", sha256.Sum256(archive))
					if scenario == "tampered" {
						archive = append(archive, 'x')
					}
					if scenario == "wrong-version" {
						archive = ingestrTestArchive(t, platform.os, entry, "other release")
					}
					requests := 0
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests++
						assert.Equal(t, "/v1.2.3/"+platform.archive, r.URL.Path)
						if scenario == "http-error" {
							w.WriteHeader(404)
							return
						}
						_, _ = w.Write(archive)
					}))
					defer server.Close()
					dir := t.TempDir()
					destination := filepath.Join(dir, name)
					require.NoError(t, os.WriteFile(destination, []byte("original"), 0o755))
					r := ingestrInstallerRuntime{
						goos: platform.os, goarch: platform.arch, httpClient: server.Client(), releaseDownloadURL: server.URL,
						hashes:    map[string]ingestrReleaseHashes{"1.2.3": {Archives: map[string]string{platform.archive: digest}}},
						findShell: func(string) (string, error) { return "", os.ErrNotExist },
					}
					version := "1.2.3"
					if scenario == "unknown" {
						version = "1.2.4"
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					if scenario == "cancelled" {
						cancel()
					}
					err := r.install(ctx, io.Discard, dir, version)
					contents, readErr := os.ReadFile(destination)
					require.NoError(t, readErr)
					if scenario == "valid" {
						require.NoError(t, err)
						assert.Equal(t, "trusted binary", string(contents))
					} else {
						require.Error(t, err)
						assert.Equal(t, "original", string(contents))
						if scenario == "tampered" || scenario == "wrong-version" {
							require.ErrorContains(t, err, "SHA-256 verification failed")
						}
						if scenario == "unknown" {
							require.ErrorContains(t, err, "no trusted embedded")
							assert.Zero(t, requests)
						}
					}
				})
			}
		})
	}
}

func TestEmbeddedIngestrHashes(t *testing.T) {
	t.Parallel()
	var hashes map[string]ingestrReleaseHashes
	require.NoError(t, json.Unmarshal(ingestrHashesJSON, &hashes))
	require.Len(t, hashes[IngestrVersionV1].Archives, 5)
	for _, digest := range hashes[IngestrVersionV1].Archives {
		assert.Regexp(t, "^[a-f0-9]{64}$", digest)
	}
	assert.Regexp(t, "^[a-f0-9]{40}$", hashes[IngestrVersionV1].InstallerCommit)
	assert.Regexp(t, "^[a-f0-9]{64}$", hashes[IngestrVersionV1].InstallerSHA256)
	_, err := ingestrArchiveName("windows", "arm64")
	require.Error(t, err)
	// The real default installer must fail closed before any network request.
	require.ErrorContains(t, runIngestrInstaller(t.Context(), io.Discard, t.TempDir(), "99.0.0"), "no trusted embedded")
}

func ingestrTestArchive(t *testing.T, goos, name, contents string) []byte {
	t.Helper()
	require.Equal(t, "windows", goos)
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	file, err := archive.Create(name)
	require.NoError(t, err)
	_, err = file.Write([]byte(contents))
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	return buffer.Bytes()
}

func TestIngestrVerifiedScript(t *testing.T) {
	t.Parallel()
	for _, platform := range []struct{ os, arch, archive string }{
		{"linux", "amd64", "ingestr_Linux_x86_64.tar.gz"},
		{"linux", "arm64", "ingestr_Linux_arm64.tar.gz"},
		{"darwin", "amd64", "ingestr_Darwin_x86_64.tar.gz"},
		{"darwin", "arm64", "ingestr_Darwin_arm64.tar.gz"},
		{"windows", "amd64", "ingestr_Windows_x86_64.zip"},
	} {
		t.Run(platform.os+platform.arch, func(t *testing.T) {
			t.Parallel()
			for _, scenario := range []string{"valid", "tampered", "http-error", "missing-script-hash", "unknown-version", "cancelled", "script-failure"} {
				t.Run(scenario, func(t *testing.T) {
					script := []byte("#!/bin/sh\nprintf trusted\n")
					commit := strings.Repeat("a", 40)
					archiveHash := strings.Repeat("b", 64)
					scriptHash := fmt.Sprintf("%x", sha256.Sum256(script))
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
						assert.Equal(t, "/"+commit+"/install.sh", request.URL.Path)
						if scenario == "http-error" {
							w.WriteHeader(http.StatusNotFound)
							return
						}
						if scenario == "tampered" {
							_, _ = w.Write([]byte("untrusted"))
							return
						}
						_, _ = w.Write(script)
					}))
					defer server.Close()
					dir := t.TempDir()
					ran := false
					if scenario == "missing-script-hash" {
						scriptHash = ""
					}
					r := ingestrInstallerRuntime{
						goos: platform.os, goarch: platform.arch,
						httpClient: server.Client(), scriptDownloadURL: server.URL,
						hashes: map[string]ingestrReleaseHashes{"1.2.3": {
							Archives:        map[string]string{platform.archive: archiveHash},
							InstallerCommit: commit, InstallerSHA256: scriptHash,
						}},
						findShell: func(string) (string, error) { return "test-sh", nil },
						runScript: func(_ context.Context, _ io.Writer, shell string, contents []byte, args []string) error {
							ran = true
							assert.Equal(t, "test-sh", shell)
							assert.Equal(t, script, contents)
							wantDir := dir
							if platform.os == "windows" {
								wantDir = strings.ReplaceAll(dir, `\`, "/")
							}
							assert.Equal(t, []string{"-s", "--", "-b", wantDir, "-s", archiveHash, "v1.2.3"}, args)
							if scenario == "script-failure" {
								return assert.AnError
							}
							return nil
						},
					}
					version := "1.2.3"
					if scenario == "unknown-version" {
						version = "1.2.4"
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					if scenario == "cancelled" {
						cancel()
					}
					err := r.install(ctx, io.Discard, dir, version)
					if scenario == "valid" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
					assert.Equal(t, scenario == "valid" || scenario == "script-failure", ran)
					if scenario == "tampered" {
						require.ErrorContains(t, err, "installer SHA-256 verification failed")
					}
					if scenario == "script-failure" {
						require.ErrorIs(t, err, assert.AnError)
					}
				})
			}
		})
	}
}

func TestRunVerifiedIngestrScript(t *testing.T) {
	t.Parallel()
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh unavailable")
	}
	var output bytes.Buffer
	// Shell metacharacters in arguments must remain literal; stdin is the exact
	// verified source, and SHELL must disable user profile modification.
	script := []byte("printf '%s|%s|%s' \"$SHELL\" \"$1\" \"$2\"\n")
	err = runVerifiedIngestrScript(t.Context(), &output, shell, script, []string{"-s", "--", "path with spaces", "$(echo injected)"})
	require.NoError(t, err)
	assert.Equal(t, "bruin-installer|path with spaces|$(echo injected)", output.String())
}
