package user

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigManager_EnsureHomeDirExists(t *testing.T) {
	t.Setenv("BRUIN_HOME", "")

	homeDir, err := os.UserHomeDir()
	require.NoError(t, err)
	assert.NotEmpty(t, homeDir)

	fs := afero.NewMemMapFs()

	c := &ConfigManager{fs: fs}

	err = c.EnsureHomeDirExists()
	require.NoError(t, err)
	assert.Equal(t, homeDir, c.userHomeDir)
	assert.Equal(t, filepath.Join(homeDir, bruinHomeDir), c.bruinHomeDir)

	fileInfo, err := fs.Stat(c.bruinHomeDir)
	require.NoError(t, err)
	assert.True(t, fileInfo.IsDir())

	// ensure repetitive calls are safe
	err = c.EnsureHomeDirExists()
	assert.NoError(t, err)
}

func TestConfigManager_EnsureVirtualenvDirExists(t *testing.T) {
	t.Setenv("BRUIN_HOME", "")

	homeDir, err := os.UserHomeDir()
	require.NoError(t, err)
	assert.NotEmpty(t, homeDir)

	fs := afero.NewMemMapFs()
	c := &ConfigManager{fs: fs}

	err = c.EnsureVirtualenvDirExists()
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(homeDir, bruinHomeDir), c.bruinHomeDir)

	fileInfo, err := fs.Stat(filepath.Join(c.bruinHomeDir, virtualEnvsPath))
	require.NoError(t, err)
	assert.True(t, fileInfo.IsDir())

	// ensure repetitive calls are safe
	err = c.EnsureVirtualenvDirExists()
	require.NoError(t, err)
}

func TestBruinHomeMigration(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	old := filepath.Join(home, ".bruin")
	destination := filepath.Join(t.TempDir(), "custom home")
	t.Setenv("BRUIN_HOME", destination)
	require.NoError(t, os.MkdirAll(filepath.Join(old, "ingestr", "v1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(old, "ingestr", "v1", "ingestr"), []byte("binary"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(old, "telemetry.json"), []byte("old"), 0o600))
	require.NoError(t, os.MkdirAll(destination, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(destination, "telemetry.json"), []byte("new"), 0o600))
	if runtime.GOOS != "windows" {
		require.NoError(t, os.Symlink(filepath.Join(old, "ingestr", "v1", "ingestr"), filepath.Join(old, "tool")))
		readOnlyDir := filepath.Join(old, "ingestr", "v1")
		require.NoError(t, os.Chmod(readOnlyDir, 0o555))
		t.Cleanup(func() { _ = os.Chmod(readOnlyDir, 0o755) })
	}

	cm := NewConfigManager(nil)
	got, err := cm.EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	require.Equal(t, destination, got)
	contents, err := os.ReadFile(filepath.Join(destination, "ingestr", "v1", "ingestr"))
	require.NoError(t, err)
	assert.Equal(t, "binary", string(contents))
	contents, err = os.ReadFile(filepath.Join(destination, "telemetry.json"))
	require.NoError(t, err)
	assert.Equal(t, "new", string(contents))
	assert.FileExists(t, filepath.Join(old, "ingestr", "v1", "ingestr"))
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(destination, "ingestr", "v1", "ingestr"))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o755), info.Mode().Perm())
		link, err := os.Readlink(filepath.Join(destination, "tool"))
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(destination, "ingestr", "v1", "ingestr"), link)
	}
	require.NoError(t, cm.EnsureVirtualenvDirExists())
	assert.DirExists(t, cm.MakeVirtualenvPath(""))
	lockBefore, err := os.Stat(filepath.Join(destination, homeMigrationLock))
	require.NoError(t, err)
	require.NoError(t, cm.RecreateHomeDir())
	lockAfter, err := os.Stat(filepath.Join(destination, homeMigrationLock))
	require.NoError(t, err)
	assert.True(t, os.SameFile(lockBefore, lockAfter), "clean must preserve the lock for waiting processes")
	_, err = NewConfigManager(nil).EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	assert.NoFileExists(t, filepath.Join(destination, "ingestr", "v1", "ingestr"))
	assert.FileExists(t, filepath.Join(old, "ingestr", "v1", "ingestr"))
}

func TestBruinHomeWithoutUserHome(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("USERPROFILE", "")
	destination := filepath.Join(t.TempDir(), "bruin")
	t.Setenv("BRUIN_HOME", destination)
	got, err := NewConfigManager(nil).EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	assert.Equal(t, destination, got)
	assert.DirExists(t, got)
}

func TestBruinHomeRejectsUserHome(t *testing.T) {
	home := t.TempDir()
	alias := strings.ToUpper(home)
	if runtime.GOOS != "windows" {
		alias = filepath.Join(t.TempDir(), "home-alias")
		require.NoError(t, os.Symlink(home, alias))
	}
	for name, userHome := range map[string]string{
		"trailing separator": home + string(filepath.Separator),
		"same directory":     alias,
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("HOME", userHome)
			t.Setenv("USERPROFILE", userHome)
			t.Setenv("BRUIN_HOME", home)
			file := filepath.Join(home, "keep-me")
			require.NoError(t, os.WriteFile(file, []byte("keep"), 0o600))
			cm := NewConfigManager(nil)
			require.ErrorContains(t, cm.EnsureHomeDirExists(), "dedicated directory")
			require.ErrorContains(t, cm.RecreateHomeDir(), "dedicated directory")
			assert.FileExists(t, file)
		})
	}
}

// Simulate another process publishing a file after migration has checked for
// its existence but before it has read the legacy contents.
type destinationAppearingFs struct {
	afero.Fs
	source, destination string
}

func (fs destinationAppearingFs) Open(name string) (afero.File, error) { //nolint:ireturn // implements afero.Fs
	if name == fs.source {
		if err := afero.WriteFile(fs.Fs, fs.destination, []byte("newer"), 0o600); err != nil {
			return nil, err
		}
	}
	return fs.Fs.Open(name)
}

func TestBruinHomeMigrationPreservesConcurrentDestination(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	destination := filepath.Join(t.TempDir(), "bruin")
	old := filepath.Join(home, ".bruin")
	// The memory filesystem exercises the rename fallback without
	// hardlinks, including a destination that appears during migration.
	for _, fs := range []afero.Fs{afero.NewOsFs(), afero.NewMemMapFs()} {
		t.Run(fs.Name(), func(t *testing.T) {
			t.Setenv("BRUIN_HOME", destination)
			require.NoError(t, fs.MkdirAll(old, 0o755))
			source := filepath.Join(old, "uv")
			target := filepath.Join(destination, "uv")
			require.NoError(t, afero.WriteFile(fs, source, []byte("legacy"), 0o755))
			require.NoError(t, afero.WriteFile(fs, filepath.Join(old, "another-tool"), []byte("complete"), 0o755))
			if fs.Name() == "OsFs" {
				var err error
				source, err = filepath.EvalSymlinks(source)
				require.NoError(t, err)
			}
			concurrent := destinationAppearingFs{Fs: fs, source: source, destination: target}
			_, err := NewConfigManager(concurrent).EnsureAndGetBruinHomeDir()
			require.NoError(t, err)
			contents, err := afero.ReadFile(fs, target)
			require.NoError(t, err)
			assert.Equal(t, "newer", string(contents))
			contents, err = afero.ReadFile(fs, filepath.Join(destination, "another-tool"))
			require.NoError(t, err)
			assert.Equal(t, "complete", string(contents))
		})
	}
}

func TestBruinHomeInsideLegacyHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	old := filepath.Join(home, ".bruin")
	destination := filepath.Join(old, "new")
	t.Setenv("BRUIN_HOME", destination)
	require.NoError(t, os.MkdirAll(old, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(old, "uv"), []byte("uv"), 0o755))
	_, err := NewConfigManager(nil).EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	assert.FileExists(t, filepath.Join(destination, "uv"))
	assert.NoDirExists(t, filepath.Join(destination, "new"))
}

type observedReadFs struct {
	afero.Fs
	observe func()
}

func (fs observedReadFs) Open(name string) (afero.File, error) { //nolint:ireturn // implements afero.Fs
	file, err := fs.Fs.Open(name)
	if err != nil {
		return nil, err
	}
	return observedReadFile{File: file, observe: fs.observe}, nil
}

type observedReadFile struct {
	afero.File
	observe func()
}

func (f observedReadFile) Read(p []byte) (int, error) {
	f.observe()
	return f.File.Read(p)
}

func TestBruinHomeMigrationPublishesCompleteFiles(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	destination := filepath.Join(t.TempDir(), "bruin")
	t.Setenv("BRUIN_HOME", destination)
	fs := afero.NewMemMapFs() // Exercise migration without hardlink support.
	old := filepath.Join(home, ".bruin")
	require.NoError(t, fs.MkdirAll(old, 0o755))
	payload := strings.Repeat("complete binary", 10000)
	require.NoError(t, afero.WriteFile(fs, filepath.Join(old, "ingestr"), []byte(payload), 0o755))
	target := filepath.Join(destination, "ingestr")
	partialFileVisible := false
	observed := observedReadFs{Fs: fs, observe: func() {
		contents, err := afero.ReadFile(fs, target)
		if err == nil && string(contents) != payload {
			partialFileVisible = true
		}
	}}
	_, err := NewConfigManager(observed).EnsureAndGetBruinHomeDir()
	require.NoError(t, err)
	assert.False(t, partialFileVisible, "stopping migration must never leave an incomplete file at the final path")
	contents, err := afero.ReadFile(fs, target)
	require.NoError(t, err)
	assert.Equal(t, payload, string(contents))
}
