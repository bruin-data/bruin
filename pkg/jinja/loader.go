package jinja

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// FileSystemLoader loads templates from a file system directory.
type FileSystemLoader struct {
	fs      afero.Fs
	baseDir string
}

// NewFileSystemLoader creates a new FileSystemLoader for the given base directory.
func NewFileSystemLoader(fs afero.Fs, baseDir string) *FileSystemLoader {
	return &FileSystemLoader{
		fs:      fs,
		baseDir: baseDir,
	}
}

// Read returns an io.Reader for the template's content.
func (l *FileSystemLoader) Read(path string) (io.Reader, error) {
	fullPath := filepath.Join(l.baseDir, path)
	content, err := afero.ReadFile(l.fs, fullPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read template file %s", path)
	}
	return strings.NewReader(string(content)), nil
}

// Resolve resolves the given path in the current context.
func (l *FileSystemLoader) Resolve(path string) (string, error) {
	fullPath := filepath.Join(l.baseDir, path)

	exists, err := afero.Exists(l.fs, fullPath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to check if template file exists: %s", path)
	}
	if !exists {
		return "", fmt.Errorf("template file not found: %s", path)
	}

	return path, nil
}

// Inherit creates a new loader relative to the given path.
func (l *FileSystemLoader) Inherit(from string) (interface{}, error) {
	newBaseDir := filepath.Join(l.baseDir, filepath.Dir(from))
	return NewFileSystemLoader(l.fs, newBaseDir), nil
}
