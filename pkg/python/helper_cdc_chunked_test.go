package python

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestConsolidatedParametersChunkedCDCOmitsSchedulerIntervals(t *testing.T) {
	t.Parallel()
	intervals := []string{"--interval-start", "2026-01-02T03:04:05Z", "--interval-end", "2026-01-03T03:04:05Z"}
	for _, tt := range []struct {
		name        string
		parameters  pipeline.ParameterMap
		fullRefresh bool
		want        []string
	}{
		{name: "chunked boolean", parameters: pipeline.ParameterMap{"cdc": true, "cdc_snapshot_chunked": true}},
		{name: "chunked string", parameters: pipeline.ParameterMap{"cdc": "true", "cdc_snapshot_chunked": "true"}},
		{name: "chunked continuous", parameters: pipeline.ParameterMap{"cdc": true, "cdc_snapshot_chunked": true, "stream": true}, want: []string{"--stream"}},
		{name: "chunked full refresh", parameters: pipeline.ParameterMap{"cdc": true, "cdc_snapshot_chunked": true}, fullRefresh: true, want: []string{"--full-refresh"}},
		{name: "disabled chunking", parameters: pipeline.ParameterMap{"cdc": true, "cdc_snapshot_chunked": false}, want: intervals},
		{name: "omitted chunking", parameters: pipeline.ParameterMap{"cdc": true}, want: intervals},
		{name: "disabled CDC", parameters: pipeline.ParameterMap{"cdc": false, "cdc_snapshot_chunked": true}, want: intervals},
		{name: "omitted CDC", parameters: pipeline.ParameterMap{"cdc_snapshot_chunked": true}, want: intervals},
		{name: "ordinary ingestion", parameters: pipeline.ParameterMap{}, want: intervals},
		{name: "ordinary continuous", parameters: pipeline.ParameterMap{"cdc": true, "stream": true}, want: append([]string{"--stream"}, intervals[:2]...)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.WithValue(t.Context(), pipeline.RunConfigStartDate, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
			ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC))
			ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, tt.fullRefresh)
			asset := &pipeline.Asset{Name: "chunked", Parameters: tt.parameters, StartDate: "2020-01-01"}
			args, err := ConsolidatedParameters(ctx, asset, []string{"ingest"}, nil)
			require.NoError(t, err)
			require.Equal(t, append([]string{"ingest"}, tt.want...), args)
		})
	}
}
