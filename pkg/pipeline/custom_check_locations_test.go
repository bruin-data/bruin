package pipeline_test

import (
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCustomCheckLocations_SQLEmbeddedYAML verifies that parsing a SQL file with

// an embedded /* @bruin … @bruin */ block annotates each custom check with its

// exact source location within the original file.

func TestCustomCheckLocations_SQLEmbeddedYAML(t *testing.T) {
	t.Parallel()

	fs := afero.NewOsFs()

	creator := pipeline.CreateTaskFromFileComments(fs)

	filePath := filepath.Join("testdata", "comments", "custom_checks_location.sql")

	absPath := path.AbsPathForTests(t, filePath)

	asset, err := creator(filePath)

	require.NoError(t, err)

	require.NotNil(t, asset)

	require.Len(t, asset.CustomChecks, 2)

	// check[0]: "row count"

	c0 := asset.CustomChecks[0]

	assert.Equal(t, "row count", c0.Name)

	require.NotNil(t, c0.SourceLocation, "SourceLocation must be set for check[0]")

	assert.Equal(t, absPath, c0.SourceLocation.File)

	assert.Equal(t, 5, c0.SourceLocation.Line,

		"'- name: row count' is at file line 5 (opening marker at line 1 + YAML line 4)")

	assert.Equal(t, 5, c0.SourceLocation.Column)

	require.NotNil(t, c0.QueryLocation, "QueryLocation must be set for check[0]")

	assert.Equal(t, absPath, c0.QueryLocation.File)

	assert.Equal(t, 6, c0.QueryLocation.Line,

		"'query:' for check[0] is at file line 6 (opening marker at line 1 + YAML line 5)")

	assert.Equal(t, 5, c0.QueryLocation.Column)

	// check[1]: "null check"

	c1 := asset.CustomChecks[1]

	assert.Equal(t, "null check", c1.Name)

	require.NotNil(t, c1.SourceLocation, "SourceLocation must be set for check[1]")

	assert.Equal(t, absPath, c1.SourceLocation.File)

	assert.Equal(t, 8, c1.SourceLocation.Line,

		"'- name: null check' is at file line 8 (opening marker at line 1 + YAML line 7)")

	assert.Equal(t, 5, c1.SourceLocation.Column)

	require.NotNil(t, c1.QueryLocation, "QueryLocation must be set for check[1]")

	assert.Equal(t, absPath, c1.QueryLocation.File)

	assert.Equal(t, 9, c1.QueryLocation.Line,

		"'query:' for check[1] is at file line 9 (opening marker at line 1 + YAML line 8)")

	assert.Equal(t, 5, c1.QueryLocation.Column)
}

// TestCustomCheckLocations_YAMLAsset verifies that parsing a .yml asset file annotates

// each custom check with its exact source location (lineOffset == 0 for pure YAML).

func TestCustomCheckLocations_YAMLAsset(t *testing.T) {
	t.Parallel()

	fs := afero.NewOsFs()

	creator := pipeline.CreateTaskFromYamlDefinition(fs)

	filePath := filepath.Join("testdata", "yaml", "task-with-custom-check-location", "task.yml")

	absPath := path.AbsPathForTests(t, filePath)

	asset, err := creator(filePath)

	require.NoError(t, err)

	require.NotNil(t, asset)

	require.Len(t, asset.CustomChecks, 2)

	// check[0]: "row count"

	c0 := asset.CustomChecks[0]

	assert.Equal(t, "row count", c0.Name)

	require.NotNil(t, c0.SourceLocation, "SourceLocation must be set for check[0]")

	assert.Equal(t, absPath, c0.SourceLocation.File)

	assert.Equal(t, 4, c0.SourceLocation.Line,

		"'- name: row count' is at YAML line 4 (lineOffset==0 for pure YAML files)")

	assert.Equal(t, 5, c0.SourceLocation.Column)

	require.NotNil(t, c0.QueryLocation, "QueryLocation must be set for check[0]")

	assert.Equal(t, absPath, c0.QueryLocation.File)

	assert.Equal(t, 5, c0.QueryLocation.Line,

		"'query:' for check[0] is at YAML line 5")

	assert.Equal(t, 5, c0.QueryLocation.Column)

	// check[1]: "null check"

	c1 := asset.CustomChecks[1]

	assert.Equal(t, "null check", c1.Name)

	require.NotNil(t, c1.SourceLocation, "SourceLocation must be set for check[1]")

	assert.Equal(t, absPath, c1.SourceLocation.File)

	assert.Equal(t, 7, c1.SourceLocation.Line,

		"'- name: null check' is at YAML line 7")

	assert.Equal(t, 5, c1.SourceLocation.Column)

	require.NotNil(t, c1.QueryLocation, "QueryLocation must be set for check[1]")

	assert.Equal(t, absPath, c1.QueryLocation.File)

	assert.Equal(t, 8, c1.QueryLocation.Line,

		"'query:' for check[1] is at YAML line 8")

	assert.Equal(t, 5, c1.QueryLocation.Column)
}

// TestCustomCheckLocations_NoChecks verifies that assets without custom checks

// are handled gracefully — the early-return path in annotateCustomCheckLocations

// must not panic and the returned asset must have an empty CustomChecks slice.

func TestCustomCheckLocations_NoChecks(t *testing.T) {
	t.Parallel()

	fs := afero.NewOsFs()

	creator := pipeline.CreateTaskFromYamlDefinition(fs)

	// task-with-no-runfile has no custom_checks section at all, which exercises

	// the len(asset.CustomChecks) == 0 early-return in annotateCustomCheckLocations.

	asset, err := creator(filepath.Join("testdata", "yaml", "task-with-no-runfile", "task.yml"))

	require.NoError(t, err)

	require.NotNil(t, asset)

	assert.Empty(t, asset.CustomChecks, "expected no custom checks for this asset")
}
