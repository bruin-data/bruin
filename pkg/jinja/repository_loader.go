package jinja

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/nikolalohinski/gonja/v2/loaders"
)

// Resolved identifiers are virtual, not host absolute paths.
const templateFileID = "\x00repo/"

type repositoryLoader struct {
	root *os.Root
	dir  string
}

func openRepositoryRoot(path string) (*os.Root, error) {
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("symlinks are not allowed in the template repository root")
	}
	return os.OpenRoot(path)
}

func (l *repositoryLoader) Resolve(name string) (string, error) {
	if l.root == nil {
		return "", fmt.Errorf("template file access requires a repository root")
	}
	var relative string
	if strings.HasPrefix(name, templateFileID) {
		relative = strings.TrimPrefix(name, templateFileID)
	} else {
		if name == "" || filepath.IsAbs(name) || filepath.VolumeName(name) != "" {
			return "", fmt.Errorf("template path must be repository-relative: %q", name)
		}
		relative = filepath.Join(l.dir, name)
	}
	if !filepath.IsLocal(relative) {
		return "", fmt.Errorf("template path escapes repository: %q", name)
	}
	// Check every component, including directories. os.Root additionally prevents
	// escaping the root if a path is replaced between this check and the read.
	current := ""
	for _, component := range strings.Split(filepath.Clean(relative), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := l.root.Lstat(current)
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("symlinks are not allowed in template paths: %q", name)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return "", fmt.Errorf("template path must contain only regular files and directories: %q", name)
		}
	}
	return templateFileID + filepath.Clean(relative), nil
}

func (l *repositoryLoader) Read(name string) (io.Reader, error) {
	id, err := l.Resolve(name)
	if err != nil {
		return nil, err
	}
	file, err := l.root.Open(strings.TrimPrefix(id, templateFileID))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("template must be a regular file: %q", name)
	}
	data, err := io.ReadAll(file)
	return bytes.NewReader(data), err
}

//nolint:ireturn
func (l *repositoryLoader) Inherit(from string) (loaders.Loader, error) {
	id, err := l.Resolve(from)
	if err != nil {
		return nil, err
	}
	return &repositoryLoader{root: l.root, dir: filepath.Dir(strings.TrimPrefix(id, templateFileID))}, nil
}

func templateFromString(source string, root *os.Root) (*exec.Template, error) {
	const inlineID = "\x00inline"
	loader, err := loaders.NewShiftedLoader(inlineID, strings.NewReader(source), &repositoryLoader{root: root})
	if err != nil {
		return nil, err
	}
	return exec.NewTemplate(inlineID, gonja.DefaultConfig, loader, gonja.DefaultEnvironment)
}
