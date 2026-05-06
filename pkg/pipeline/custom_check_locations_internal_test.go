package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestAnnotateCustomCheckLocations_SkipsNonMappingNodes verifies the fix for the
// index mis-alignment bug: when a non-mapping node (e.g. a scalar) appears in the
// custom_checks sequence, annotateCustomCheckLocations must skip it without
// advancing the CustomChecks index, so the subsequent checks are still annotated
// with the correct positions.
func TestAnnotateCustomCheckLocations_SkipsNonMappingNodes(t *testing.T) {
	t.Parallel()

	// Build a minimal YAML document with two valid mapping nodes and a scalar
	// interspersed between them:
	//
	//   custom_checks:
	//     - name: check_one       <- mapping node (index 0 in Content)
	//       query: select 1
	//     - "stray scalar"        <- scalar node  (index 1 in Content, must be skipped)
	//     - name: check_two       <- mapping node (index 2 in Content)
	//       query: select 2
	//
	// Without the ccIdx fix the second mapping node (check_two) would be annotated
	// as CustomChecks[2], but there are only 2 checks so that slot doesn't exist —
	// and CustomChecks[1] would remain un-annotated.
	const src = `custom_checks:
  - name: check_one
    query: select 1
  - "stray scalar"
  - name: check_two
    query: select 2
`
	// Parse into a yaml.Node tree.
	var root yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(src), &root))

	// The asset has exactly 2 custom checks (the scalar is not a check).
	asset := &Asset{
		CustomChecks: []CustomCheck{
			{Name: "check_one"},
			{Name: "check_two"},
		},
	}

	annotateCustomCheckLocations(asset, &root, "fake.yml", 0)

	// check_one (first mapping) must be annotated.
	require.NotNil(t, asset.CustomChecks[0].SourceLocation,
		"check_one must have a SourceLocation")
	assert.Equal(t, "fake.yml", asset.CustomChecks[0].SourceLocation.File)

	// check_two (third YAML node, second mapping) must also be annotated.
	// Before the fix, this would be nil because ccIdx would have been incremented
	// for the scalar node, leaving CustomChecks[1] never reached.
	require.NotNil(t, asset.CustomChecks[1].SourceLocation,
		"check_two must have a SourceLocation — ccIdx must not advance for non-mapping nodes")
	assert.Equal(t, "fake.yml", asset.CustomChecks[1].SourceLocation.File)

	// Both query locations must be set too.
	require.NotNil(t, asset.CustomChecks[0].QueryLocation,
		"check_one must have a QueryLocation")
	require.NotNil(t, asset.CustomChecks[1].QueryLocation,
		"check_two must have a QueryLocation")
}
