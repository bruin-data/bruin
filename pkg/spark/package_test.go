package spark

import (
	"archive/zip"
	"bytes"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExclude(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"README.md is excluded", "README.md", true},
		{".bruin.yml is excluded", ".bruin.yml", true},
		{"pipeline.yml is excluded", "pipeline.yml", true},
		{"pipeline.yaml is excluded", "pipeline.yaml", true},
		{"nested README.md is excluded", "subdir/README.md", true},
		{".venv directory is excluded", ".venv/lib/python", true},
		{"venv directory is excluded", "venv/lib/python", true},
		{"root logs directory is excluded", "logs/output.log", true},
		{".git directory is excluded", ".git/config", true},
		{"regular python file is not excluded", "main.py", false},
		{"nested python file is not excluded", "src/utils.py", false},
		{"regular directory is not excluded", "src/", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := Exclude(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPackageContext(t *testing.T) {
	t.Parallel()

	// Create a mock filesystem
	mockFS := fstest.MapFS{
		"main.py":          &fstest.MapFile{Data: []byte("print('hello')")},
		"utils/helper.py":  &fstest.MapFile{Data: []byte("def help(): pass")},
		"README.md":        &fstest.MapFile{Data: []byte("# Readme")},
		".bruin.yml":       &fstest.MapFile{Data: []byte("config: true")},
		".venv/lib/pkg.py": &fstest.MapFile{Data: []byte("# should be excluded")},
		".git/config":      &fstest.MapFile{Data: []byte("# should be excluded")},
		"logs/output.log":  &fstest.MapFile{Data: []byte("# should be excluded")},
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	err := PackageContext(zw, mockFS)
	require.NoError(t, err)

	err = zw.Close()
	require.NoError(t, err)

	// Read the created zip to verify contents
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	fileNames := make(map[string]bool)
	for _, f := range zr.File {
		fileNames[f.Name] = true
	}

	// Verify included files
	assert.True(t, fileNames["main.py"], "main.py should be included")
	assert.True(t, fileNames["utils/helper.py"], "utils/helper.py should be included")
	assert.True(t, fileNames["utils/__init__.py"], "utils/__init__.py should be auto-created")
	// Root directory gets __init__.py with path "./__init__.py"
	assert.True(t, fileNames["./__init__.py"] || fileNames["__init__.py"], "__init__.py in root should be auto-created")

	// Verify excluded files
	assert.False(t, fileNames["README.md"], "README.md should be excluded")
	assert.False(t, fileNames[".bruin.yml"], ".bruin.yml should be excluded")
	assert.False(t, fileNames[".venv/lib/pkg.py"], ".venv files should be excluded")
	assert.False(t, fileNames[".git/config"], ".git files should be excluded")
	assert.False(t, fileNames["logs/output.log"], "logs files should be excluded")
}

func TestPackageContextWithEmptyFS(t *testing.T) {
	t.Parallel()

	mockFS := fstest.MapFS{}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	err := PackageContext(zw, mockFS)
	require.NoError(t, err)

	err = zw.Close()
	require.NoError(t, err)
}

func TestPackageContextWalkError(t *testing.T) {
	t.Parallel()

	// Create a filesystem that returns an error
	errFS := &errorFS{err: fs.ErrPermission}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	err := PackageContext(zw, errFS)
	assert.Error(t, err)
}

type errorFS struct {
	err error
}

func (e *errorFS) Open(name string) (fs.File, error) {
	return nil, e.err
}
