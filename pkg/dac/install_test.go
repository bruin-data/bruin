package dac

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testVersion = "9.8.7"

var fakeBinary = []byte("#!/bin/sh\necho fake dac\n")

// fakeRelease serves dac release archives the way GitHub does and counts the
// downloads it handles.
type fakeRelease struct {
	server    *httptest.Server
	archives  map[string][]byte
	downloads atomic.Int32
	// barrier, when set, holds every download until that many have started.
	barrier *barrier
}

type barrier struct {
	remaining atomic.Int32
	open      chan struct{}
}

func newBarrier(n int32) *barrier {
	b := &barrier{open: make(chan struct{})}
	b.remaining.Store(n)
	return b
}

func (b *barrier) wait() {
	if b.remaining.Add(-1) == 0 {
		close(b.open)
	}
	select {
	case <-b.open:
	case <-time.After(30 * time.Second):
	}
}

// newFakeRelease starts a fake release server. configure runs before the server
// starts, so request handlers never race with test setup.
func newFakeRelease(t *testing.T, configure ...func(*fakeRelease)) *fakeRelease {
	t.Helper()
	release := &fakeRelease{archives: map[string][]byte{
		"dac_Linux_x86_64.tar.gz": tarGzArchive(t, map[string][]byte{"LICENSE": []byte("license"), "dac": fakeBinary}),
		"dac_Windows_x86_64.zip":  zipArchive(t, map[string][]byte{"LICENSE": []byte("license"), "dac.exe": fakeBinary}),
	}}
	for _, fn := range configure {
		fn(release)
	}
	release.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		release.downloads.Add(1)
		if release.barrier != nil {
			release.barrier.wait()
		}
		for name, body := range release.archives {
			if r.URL.Path == "/v"+testVersion+"/"+name {
				_, _ = w.Write(body)
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(release.server.Close)
	return release
}

func (f *fakeRelease) pins() Release {
	archives := map[string]string{}
	for name, body := range f.archives {
		sum := sha256.Sum256(body)
		archives[name] = hex.EncodeToString(sum[:])
	}
	return Release{Version: testVersion, Archives: archives}
}

func (f *fakeRelease) installer(homeDir, goos, goarch string) *Installer {
	return &Installer{
		Release:    f.pins(),
		HomeDir:    homeDir,
		GOOS:       goos,
		GOARCH:     goarch,
		BaseURL:    f.server.URL,
		HTTPClient: f.server.Client(),
		Output:     &bytes.Buffer{},
	}
}

func tarGzArchive(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}))
		_, err := tw.Write(body)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return buffer.Bytes()
}

func zipArchive(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	zw := zip.NewWriter(&buffer)
	for name, body := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = w.Write(body)
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	return buffer.Bytes()
}

// assertNoInstallLeftovers checks that a failed install left nothing behind.
func assertNoInstallLeftovers(t *testing.T, homeDir string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(homeDir, "dac"))
	if os.IsNotExist(err) {
		return
	}
	require.NoError(t, err)
	assert.Empty(t, entries, "a failed install must not leave files in the dac directory")
}

func TestPinnedReleaseCoversEveryArchive(t *testing.T) {
	t.Parallel()

	release, err := PinnedRelease()
	require.NoError(t, err)
	assert.Regexp(t, `^\d+\.\d+\.\d+$`, release.Version)

	sha := regexp.MustCompile(`^[a-f0-9]{64}$`)
	expected := []string{
		"dac_Darwin_arm64.tar.gz",
		"dac_Darwin_x86_64.tar.gz",
		"dac_Linux_arm64.tar.gz",
		"dac_Linux_x86_64.tar.gz",
		"dac_Windows_x86_64.zip",
	}
	assert.Len(t, release.Archives, len(expected))
	for _, name := range expected {
		assert.Regexp(t, sha, release.Archives[name], name)
	}
}

func TestArchiveName(t *testing.T) {
	t.Parallel()

	for platform, expected := range map[[2]string]string{
		{"darwin", "arm64"}:  "dac_Darwin_arm64.tar.gz",
		{"darwin", "amd64"}:  "dac_Darwin_x86_64.tar.gz",
		{"linux", "arm64"}:   "dac_Linux_arm64.tar.gz",
		{"linux", "amd64"}:   "dac_Linux_x86_64.tar.gz",
		{"windows", "amd64"}: "dac_Windows_x86_64.zip",
	} {
		name, err := ArchiveName(platform[0], platform[1])
		require.NoError(t, err)
		assert.Equal(t, expected, name)
	}

	for _, platform := range [][2]string{{"windows", "arm64"}, {"linux", "386"}, {"freebsd", "amd64"}} {
		_, err := ArchiveName(platform[0], platform[1])
		require.ErrorContains(t, err, "dac has no release for "+platform[0]+"/"+platform[1])
	}
}

func TestEnsureInstalled_InstallsThenReusesWithoutNetwork(t *testing.T) {
	t.Parallel()

	release := newFakeRelease(t)
	homeDir := t.TempDir()
	installer := release.installer(homeDir, "linux", "amd64")

	path, err := installer.EnsureInstalled(t.Context())
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(homeDir, "dac", testVersion, "dac"), path)
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fakeBinary, content)
	if info, err := os.Stat(path); assert.NoError(t, err) && runtimeSupportsExecBit() {
		assert.Equal(t, os.FileMode(0o755), info.Mode().Perm()&0o755)
	}
	assert.Contains(t, installer.Output.(*bytes.Buffer).String(), "Installing dac v"+testVersion)
	assert.Equal(t, int32(1), release.downloads.Load())

	// Later runs reuse the install, even with the release server gone.
	release.server.Close()
	again := release.installer(homeDir, "linux", "amd64")
	path2, err := again.EnsureInstalled(t.Context())
	require.NoError(t, err)
	assert.Equal(t, path, path2)
	assert.Empty(t, again.Output.(*bytes.Buffer).String())

	entries, err := os.ReadDir(filepath.Join(homeDir, "dac"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "the temporary install directory must be removed")
	assert.Equal(t, testVersion, entries[0].Name())
}

func TestEnsureInstalled_ExtractsWindowsZip(t *testing.T) {
	t.Parallel()

	release := newFakeRelease(t)
	homeDir := t.TempDir()

	path, err := release.installer(homeDir, "windows", "amd64").EnsureInstalled(t.Context())
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(homeDir, "dac", testVersion, "dac.exe"), path)
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fakeBinary, content)
}

func TestEnsureInstalled_ChecksumMismatchLeavesNothing(t *testing.T) {
	t.Parallel()

	release := newFakeRelease(t)
	homeDir := t.TempDir()
	installer := release.installer(homeDir, "linux", "amd64")
	installer.Release.Archives["dac_Linux_x86_64.tar.gz"] = hex.EncodeToString(make([]byte, sha256.Size))

	_, err := installer.EnsureInstalled(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to install dac v"+testVersion)
	assert.Contains(t, err.Error(), "SHA-256 mismatch for dac_Linux_x86_64.tar.gz")
	assert.Contains(t, err.Error(), "To install dac v"+testVersion+" manually, download https://github.com/bruin-data/dac/releases/download/v"+testVersion+"/dac_Linux_x86_64.tar.gz")
	assert.Contains(t, err.Error(), filepath.Join(homeDir, "dac", testVersion, "dac"))
	assert.Contains(t, err.Error(), "BRUIN_DAC_BINARY")
	assertNoInstallLeftovers(t, homeDir)
}

func TestEnsureInstalled_DownloadFailuresLeaveNothing(t *testing.T) {
	t.Parallel()

	t.Run("server error", func(t *testing.T) {
		t.Parallel()
		pins := newFakeRelease(t).pins()
		release := newFakeRelease(t, func(f *fakeRelease) { f.archives = map[string][]byte{} })
		installer := release.installer(t.TempDir(), "linux", "amd64")
		installer.Release = pins

		_, err := installer.EnsureInstalled(t.Context())
		require.ErrorContains(t, err, "server returned 404 Not Found")
		assert.Contains(t, err.Error(), "manually")
		assertNoInstallLeftovers(t, installer.HomeDir)
	})

	t.Run("offline", func(t *testing.T) {
		t.Parallel()
		release := newFakeRelease(t)
		installer := release.installer(t.TempDir(), "linux", "amd64")
		release.server.Close()

		_, err := installer.EnsureInstalled(t.Context())
		require.ErrorContains(t, err, "download failed")
		assert.Contains(t, err.Error(), release.server.URL+"/v"+testVersion+"/dac_Linux_x86_64.tar.gz")
		assert.Contains(t, err.Error(), "manually")
		assertNoInstallLeftovers(t, installer.HomeDir)
	})

	t.Run("archive without dac", func(t *testing.T) {
		t.Parallel()
		release := newFakeRelease(t, func(f *fakeRelease) {
			f.archives["dac_Linux_x86_64.tar.gz"] = tarGzArchive(t, map[string][]byte{"README.md": []byte("readme")})
		})
		installer := release.installer(t.TempDir(), "linux", "amd64")

		_, err := installer.EnsureInstalled(t.Context())
		require.ErrorContains(t, err, "the dac archive does not contain dac")
		assertNoInstallLeftovers(t, installer.HomeDir)
	})

	t.Run("cancelled", func(t *testing.T) {
		t.Parallel()
		release := newFakeRelease(t)
		installer := release.installer(t.TempDir(), "linux", "amd64")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := installer.EnsureInstalled(ctx)
		require.ErrorIs(t, err, context.Canceled)
		assert.NotContains(t, err.Error(), "manually")
		assertNoInstallLeftovers(t, installer.HomeDir)
	})
}

func TestEnsureInstalled_UnsupportedPlatform(t *testing.T) {
	t.Parallel()

	release := newFakeRelease(t)
	installer := release.installer(t.TempDir(), "windows", "arm64")

	_, err := installer.EnsureInstalled(t.Context())
	require.ErrorContains(t, err, "dac has no release for windows/arm64")
	assert.Contains(t, err.Error(), "BRUIN_DAC_BINARY")
	assert.Equal(t, int32(0), release.downloads.Load())
	assertNoInstallLeftovers(t, installer.HomeDir)
}

func TestEnsureInstalled_MissingPin(t *testing.T) {
	t.Parallel()

	release := newFakeRelease(t)
	installer := release.installer(t.TempDir(), "darwin", "arm64")

	_, err := installer.EnsureInstalled(t.Context())
	require.ErrorContains(t, err, "no pinned SHA-256 for dac_Darwin_arm64.tar.gz")
	assert.Equal(t, int32(0), release.downloads.Load())
}

func TestEnsureInstalled_ConcurrentInstallsInOneProcess(t *testing.T) {
	t.Parallel()

	release := newFakeRelease(t)
	homeDir := t.TempDir()

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() {
			_, errs[i] = release.installer(homeDir, "linux", "amd64").EnsureInstalled(t.Context())
		})
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), release.downloads.Load(), "the in-process lock must serialize installs")
}

// TestEnsureInstalled_ConcurrentInstallsAcrossProcesses starts several test
// processes that all install into one empty home directory at once, as
// concurrent first runs of `bruin dac` would.
func TestEnsureInstalled_ConcurrentInstallsAcrossProcesses(t *testing.T) {
	t.Parallel()

	const processes = 4
	release := newFakeRelease(t, func(f *fakeRelease) { f.barrier = newBarrier(processes) })
	homeDir := t.TempDir()
	sum := sha256.Sum256(release.archives["dac_Linux_x86_64.tar.gz"])

	commands := make([]*exec.Cmd, processes)
	outputs := make([]*bytes.Buffer, len(commands))
	for i := range commands {
		outputs[i] = &bytes.Buffer{}
		commands[i] = helperCommand(
			t, "install",
			"DAC_TEST_SERVER="+release.server.URL,
			"DAC_TEST_HOME="+homeDir,
			"DAC_TEST_SHA="+hex.EncodeToString(sum[:]),
		)
		commands[i].Stdout = outputs[i]
		commands[i].Stderr = outputs[i]
		require.NoError(t, commands[i].Start())
	}
	for i, command := range commands {
		require.NoError(t, command.Wait(), outputs[i].String())
	}

	// The barrier only opens once every process is mid-download.
	assert.Equal(t, int32(processes), release.downloads.Load())
	content, err := os.ReadFile(filepath.Join(homeDir, "dac", testVersion, "dac"))
	require.NoError(t, err)
	assert.Equal(t, fakeBinary, content)
	entries, err := os.ReadDir(filepath.Join(homeDir, "dac"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temporary install directories may remain")
}

func runtimeSupportsExecBit() bool {
	return os.PathSeparator == '/'
}
