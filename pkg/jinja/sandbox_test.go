package jinja

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const scopeSecret = "TOP_SECRET_HOST_FILE_CONTENTS"

func templateTags(path string) map[string]string {
	return map[string]string{
		"include": `{% include "` + path + `" %}`,
		"import":  `{% import "` + path + `" as m %}`,
		"from":    `{% from "` + path + `" import thing %}`,
		"extends": `{% extends "` + path + `" %}`,
	}
}

//nolint:paralleltest // t.Setenv forbids parallel subtests
func TestRenderer_RestrictedRejectsEscapingPaths(t *testing.T) {
	t.Setenv(RestrictTemplateFilesystemEnv, "true")

	dir := t.TempDir()
	absPath := filepath.Join(dir, "secret.txt")
	require.NoError(t, os.WriteFile(absPath, []byte(scopeSecret), 0o600))

	// Jinja string literals treat backslashes as escapes, so use forward slashes.
	templatePaths := map[string]string{"absolute": filepath.ToSlash(absPath)}
	cwd, err := os.Getwd()
	require.NoError(t, err)
	if rel, relErr := filepath.Rel(cwd, absPath); relErr == nil && strings.HasPrefix(rel, "..") {
		templatePaths["traversal"] = filepath.ToSlash(rel)
	}

	for pathName, path := range templatePaths {
		for tag, tmpl := range templateTags(path) {
			t.Run(pathName+"/"+tag, func(t *testing.T) {
				out, err := NewRenderer(Context{}).Render(tmpl)
				require.Error(t, err)
				require.Equal(t, errTemplateFileAccessDenied.Error(), err.Error())
				require.NotContains(t, out, scopeSecret)
			})
		}
	}
}

func TestRenderer_RestrictedRejectsSymlinkEscape(t *testing.T) {
	t.Setenv(RestrictTemplateFilesystemEnv, "true")

	outside := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(outside, []byte(scopeSecret), 0o600))

	link, err := os.CreateTemp(".", "jinja_link_*")
	require.NoError(t, err)
	require.NoError(t, link.Close())
	require.NoError(t, os.Remove(link.Name()))
	t.Cleanup(func() { os.Remove(link.Name()) })
	if err := os.Symlink(outside, link.Name()); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	out, err := NewRenderer(Context{}).Render(`{% include "` + filepath.ToSlash(filepath.Base(link.Name())) + `" %}`)
	require.Error(t, err)
	require.Equal(t, errTemplateFileAccessDenied.Error(), err.Error())
	require.NotContains(t, out, scopeSecret)
}

func TestRenderer_RestrictedAllowsFilesWithinWorkingDirectory(t *testing.T) {
	t.Setenv(RestrictTemplateFilesystemEnv, "true")

	f, err := os.CreateTemp(".", "jinja_scope_*.txt")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(f.Name()) })
	_, err = f.WriteString(scopeSecret)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	out, err := NewRenderer(Context{}).Render(`{% include "` + filepath.ToSlash(filepath.Base(f.Name())) + `" %}`)
	require.NoError(t, err)
	require.Contains(t, out, scopeSecret)
}

func TestRenderer_UnrestrictedByDefaultLoadsAnyFile(t *testing.T) {
	t.Setenv(RestrictTemplateFilesystemEnv, "")

	dir := t.TempDir()
	absPath := filepath.Join(dir, "secret.txt")
	require.NoError(t, os.WriteFile(absPath, []byte(scopeSecret), 0o600))

	out, err := NewRenderer(Context{}).Render(`{% include "` + filepath.ToSlash(absPath) + `" %}`)
	require.NoError(t, err)
	require.Contains(t, out, scopeSecret)
}

func TestScopedLoader_AllowsPathsWhenRootIsFilesystemRoot(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("POSIX filesystem root only")
	}

	l := &scopedLoader{root: "/", dir: "/"}
	resolved, err := l.resolve("etc/hostname")
	require.NoError(t, err)
	require.Equal(t, "/etc/hostname", resolved)
}

func TestRenderer_NormalTemplatesStillRender(t *testing.T) {
	t.Parallel()

	out, err := NewRenderer(Context{}).Render(`{% for i in [1, 2, 3] %}{{ i }}{% endfor %}`)
	require.NoError(t, err)
	require.Equal(t, "123", out)
}
