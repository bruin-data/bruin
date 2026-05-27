package ingestr

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func TestAppendQueryAnnotations(t *testing.T) {
	t.Parallel()

	ti := &scheduler.AssetInstance{
		Pipeline: &pipeline.Pipeline{Name: "shopify"},
		Asset:    &pipeline.Asset{Name: "raw.orders"},
	}
	v1 := resolvedEngine{family: versionFamilyV1, ingestrVersion: python.IngestrVersionV1}
	v0 := resolvedEngine{family: versionFamilyV0, ingestrVersion: python.IngestrVersionV0}

	t.Run("added for the v1 engine", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest", "--dest-table", "raw.orders"}
		got := appendQueryAnnotations(base, v1, ti)
		require.Equal(t, []string{
			"ingest", "--dest-table", "raw.orders",
			"--query-annotations", `{"asset":"raw.orders","pipeline":"shopify"}`,
		}, got)
	})

	t.Run("omitted for the legacy v0 engine", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest", "--dest-table", "raw.orders"}
		require.Equal(t, base, appendQueryAnnotations(base, v0, ti))
	})

	t.Run("omitted when pipeline/asset identity is incomplete", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest", "--dest-table", "raw.orders"}
		incomplete := &scheduler.AssetInstance{
			Pipeline: &pipeline.Pipeline{}, // no name
			Asset:    &pipeline.Asset{Name: "raw.orders"},
		}
		require.Equal(t, base, appendQueryAnnotations(base, v1, incomplete))
	})
}
