package gong

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

// ─── shared fixtures ─────────────────────────────────────────────────────────

// fakeBinaryContent is a small, deterministic blob used as the stand-in gong
// executable. Using a package-level var (read-only in tests) avoids allocating
// it repeatedly while keeping all tests hermetic.
var fakeBinaryContent = []byte("fake gong binary content for integration tests")

// checksumManifestFor returns a sha256sum-format manifest whose single entry
// covers binary under the current platform's artifact name (e.g. gong_linux_amd64).
// This mirrors the format that GoReleaser publishes as checksums.txt.
func checksumManifestFor(binary []byte) string {
	sum := sha256.Sum256(binary)
	artifactName := fmt.Sprintf("gong_%s_%s", getOSName(), getArchName())
	return hex.EncodeToString(sum[:]) + "  " + artifactName + "\n"
}

// installBinaryPath reconstructs the on-disk path where Checker places the
// binary when installDir is set. It mirrors (*Checker).binaryPath exactly so
// there is a single authoritative location per test.
func installBinaryPath(installDir, version string) string {
	name := "gong-" + version
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(installDir, name)
}

// newInstallTestServer starts an httptest.Server that routes the two URLs
// Checker derives from its baseURL:
//
//   - /releases/{version}/{os}/gong_{arch}  → serves binary with HTTP 200
//   - /releases/{version}/checksums.txt     → serves checksumBody with checksumStatus
//
// Requests for any other path are treated as test failures.
// checksumStatus == 0 is normalised to http.StatusOK.
// The server is automatically closed when the test finishes.
func newInstallTestServer(
	t *testing.T,
	version string,
	binary []byte,
	checksumBody string,
	checksumStatus int,
) *httptest.Server {
	t.Helper()

	if checksumStatus == 0 {
		checksumStatus = http.StatusOK
	}
	binaryURLPath := fmt.Sprintf("/releases/%s/%s/gong_%s", version, getOSName(), getArchName())
	checksumURLPath := fmt.Sprintf("/releases/%s/checksums.txt", version)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case binaryURLPath:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(binary)
		case checksumURLPath:
			w.WriteHeader(checksumStatus)
			_, _ = io.WriteString(w, checksumBody)
		default:
			// Any unexpected request is a test bug, not a product bug.
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// ─── integration tests ───────────────────────────────────────────────────────

const installTestVersion = "v0.0.0-test"

// TestEnsureGongInstalled_ValidChecksumInstallsBinary is the happy-path test:
// the checksum manifest matches the downloaded binary, so EnsureGongInstalled
// must return no error and write the binary to the install directory with the
// exact same bytes that the server sent.
func TestEnsureGongInstalled_ValidChecksumInstallsBinary(t *testing.T) {
	t.Parallel()

	installDir := t.TempDir()
	manifest := checksumManifestFor(fakeBinaryContent)
	srv := newInstallTestServer(t, installTestVersion, fakeBinaryContent, manifest, 0)
	checker := &Checker{baseURL: srv.URL, installDir: installDir}

	gotPath, err := checker.EnsureGongInstalled(context.Background(), installTestVersion)

	require.NoError(t, err, "valid checksum must not produce an error")
	expectedPath := installBinaryPath(installDir, installTestVersion)
	assert.Equal(t, expectedPath, gotPath, "returned path must equal the final install destination")

	// Content integrity: bytes on disk must exactly match what the server served.
	gotBytes, readErr := os.ReadFile(expectedPath)
	require.NoError(t, readErr, "installed binary must be readable at the returned path")
	assert.Equal(t, fakeBinaryContent, gotBytes, "installed binary content must equal the downloaded binary")
}

// TestEnsureGongInstalled_InvalidChecksumBlocksInstall verifies that when the
// checksum manifest contains a hash for different content, EnsureGongInstalled
// returns an error that names the mismatch, and leaves no file at the final
// install path (the temp file is cleaned up automatically by the deferred
// os.Remove inside downloadGong).
func TestEnsureGongInstalled_InvalidChecksumBlocksInstall(t *testing.T) {
	t.Parallel()

	installDir := t.TempDir()
	// Build a manifest whose hash is for content that differs from fakeBinaryContent.
	wrongManifest := checksumManifestFor([]byte("this is not the binary that was downloaded"))
	srv := newInstallTestServer(t, installTestVersion, fakeBinaryContent, wrongManifest, 0)
	checker := &Checker{baseURL: srv.URL, installDir: installDir}

	_, err := checker.EnsureGongInstalled(context.Background(), installTestVersion)

	require.Error(t, err, "a checksum mismatch must produce an error")
	assert.Contains(t, err.Error(), "checksum mismatch",
		"error message must identify the cause as a checksum mismatch")
	_, statErr := os.Stat(installBinaryPath(installDir, installTestVersion))
	assert.True(t, os.IsNotExist(statErr),
		"binary must not exist at the final install path after a mismatch")
}

// TestEnsureGongInstalled_MissingChecksumEntryBlocksInstall verifies that when
// the checksum manifest does not contain an entry for the current platform's
// artifact name, EnsureGongInstalled fails before installing anything.
func TestEnsureGongInstalled_MissingChecksumEntryBlocksInstall(t *testing.T) {
	t.Parallel()

	installDir := t.TempDir()
	// Manifest has a valid entry but for a completely different artifact name,
	// so the lookup for the current platform's artifact returns "not found".
	unrelatedHash := hashOf(fakeBinaryContent)
	manifestWithWrongArtifact := unrelatedHash + "  gong_completely_different_arch\n"
	srv := newInstallTestServer(t, installTestVersion, fakeBinaryContent, manifestWithWrongArtifact, 0)
	checker := &Checker{baseURL: srv.URL, installDir: installDir}

	_, err := checker.EnsureGongInstalled(context.Background(), installTestVersion)

	require.Error(t, err, "a missing checksum entry must produce an error")
	assert.Contains(t, err.Error(), "checksum entry not found",
		"error message must report that no matching checksum was found in the manifest")
	_, statErr := os.Stat(installBinaryPath(installDir, installTestVersion))
	assert.True(t, os.IsNotExist(statErr),
		"binary must not exist at the final install path when its checksum entry is absent")
}

// TestEnsureGongInstalled_ChecksumServerFailureBlocksInstall verifies that an
// HTTP 500 from the checksum endpoint prevents the binary from reaching its
// final install path. The binary download itself succeeds; only the checksum
// fetch fails — this is the strongest proof that verification is mandatory.
func TestEnsureGongInstalled_ChecksumServerFailureBlocksInstall(t *testing.T) {
	t.Parallel()

	installDir := t.TempDir()
	srv := newInstallTestServer(t, installTestVersion, fakeBinaryContent, "", http.StatusInternalServerError)
	checker := &Checker{baseURL: srv.URL, installDir: installDir}

	_, err := checker.EnsureGongInstalled(context.Background(), installTestVersion)

	require.Error(t, err,
		"an HTTP error on the checksum endpoint must propagate as an error")
	_, statErr := os.Stat(installBinaryPath(installDir, installTestVersion))
	assert.True(t, os.IsNotExist(statErr),
		"binary must not exist at the final install path after a checksum server failure")
}
