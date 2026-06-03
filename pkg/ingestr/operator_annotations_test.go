package ingestr

import (
	"context"
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

	// annotated returns a context carrying the run-level annotations exactly as
	// `bruin run --query-annotations` / BRUIN_QUERY_ANNOTATIONS would set them.
	annotated := func(payload string) context.Context {
		return context.WithValue(context.Background(), pipeline.RunConfigQueryAnnotations, payload)
	}

	t.Run("opt-in: nothing forwarded without run-level annotations", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest", "--dest-table", "raw.orders"}
		got, err := appendQueryAnnotations(context.Background(), base, v1, ti)
		require.NoError(t, err)
		require.Equal(t, base, got)
	})

	t.Run("default sentinel forwards pipeline/asset identity", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest"}
		got, err := appendQueryAnnotations(annotated("default"), base, v1, ti)
		require.NoError(t, err)
		require.Equal(t, []string{
			"ingest",
			"--query-annotations", `{"asset":"raw.orders","pipeline":"shopify"}`,
		}, got)
	})

	t.Run("merges run-level annotations (project/run_id) from context", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest"}
		got, err := appendQueryAnnotations(annotated(`{"project":"3tlabs","run_id":"scheduled__2026-06-01T00:00:00+00:00"}`), base, v1, ti)
		require.NoError(t, err)
		require.Equal(t, []string{
			"ingest",
			"--query-annotations", `{"asset":"raw.orders","pipeline":"shopify","project":"3tlabs","run_id":"scheduled__2026-06-01T00:00:00+00:00"}`,
		}, got)
	})

	t.Run("preserves non-string run-level values like try_number", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest"}
		got, err := appendQueryAnnotations(annotated(`{"project":"3tlabs","run_id":"manual__2026-06-02T12:50:51+00:00","try_number":0}`), base, v1, ti)
		require.NoError(t, err)
		require.Equal(t, []string{
			"ingest",
			"--query-annotations", `{"asset":"raw.orders","pipeline":"shopify","project":"3tlabs","run_id":"manual__2026-06-02T12:50:51+00:00","try_number":0}`,
		}, got)
	})

	t.Run("propagates an error on invalid run-level annotations JSON", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest"}
		got, err := appendQueryAnnotations(annotated(`{not valid json`), base, v1, ti)
		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("omitted for the legacy v0 engine", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest", "--dest-table", "raw.orders"}
		got, err := appendQueryAnnotations(annotated("default"), base, v0, ti)
		require.NoError(t, err)
		require.Equal(t, base, got)
	})

	t.Run("omitted when pipeline/asset identity is incomplete", func(t *testing.T) {
		t.Parallel()
		base := []string{"ingest", "--dest-table", "raw.orders"}
		incomplete := &scheduler.AssetInstance{
			Pipeline: &pipeline.Pipeline{}, // no name
			Asset:    &pipeline.Asset{Name: "raw.orders"},
		}
		got, err := appendQueryAnnotations(annotated("default"), base, v1, incomplete)
		require.NoError(t, err)
		require.Equal(t, base, got)
	})
}
