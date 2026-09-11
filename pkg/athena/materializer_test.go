package athena

import (
	"bytes"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestLogIfFullRefreshAndDDL(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name         string
		fullRefresh  bool
		strategy     pipeline.MaterializationStrategy
		writer       interface{}
		expectErr    string
		expectOutput string
	}{
		{
			name:        "fullRefresh false",
			fullRefresh: false,
			strategy:    pipeline.MaterializationStrategyDDL,
			writer:      &bytes.Buffer{},
		},
		{
			name:        "strategy not DDL",
			fullRefresh: true,
			strategy:    pipeline.MaterializationStrategyCreateReplace,
			writer:      &bytes.Buffer{},
		},
		{
			name:        "writer is nil",
			fullRefresh: true,
			strategy:    pipeline.MaterializationStrategyDDL,
			writer:      nil,
			expectErr:   "no writer found in context",
		},
		{
			name:        "writer not io.Writer",
			fullRefresh: true,
			strategy:    pipeline.MaterializationStrategyDDL,
			writer:      123,
			expectErr:   "writer is not an io.Writer",
		},
		{
			name:         "all conditions met",
			fullRefresh:  true,
			strategy:     pipeline.MaterializationStrategyDDL,
			writer:       &bytes.Buffer{},
			expectOutput: "Full refresh detected, but DDL strategy is in use — table will NOT be dropped or recreated.\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mat := pipeline.Materialization{Strategy: tc.strategy}
			asset := &pipeline.Asset{Materialization: mat}
			m := &Materializer{fullRefresh: tc.fullRefresh}

			err := m.LogIfFullRefreshAndDDL(tc.writer, asset)
			if tc.expectErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErr)
			} else {
				require.NoError(t, err)
				if buf, ok := tc.writer.(*bytes.Buffer); ok && tc.expectOutput != "" {
					require.Equal(t, tc.expectOutput, buf.String())
				}
			}
		})
	}
}

func TestMaterializer_RenderInitialTable(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "analytics.events",
		Materialization: pipeline.Materialization{
			Type:        pipeline.MaterializationTypeTable,
			Strategy:    pipeline.MaterializationStrategyTimeInterval,
			PartitionBy: "event_date",
		},
	}

	queries, err := NewMaterializer(false).RenderInitialTable(
		asset,
		"SELECT event_date, event_name FROM raw_events;",
		"s3://bucket/results",
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"CREATE TABLE analytics.events WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/results/analytics.events', partitioning = ARRAY['event_date']) AS SELECT event_date, event_name FROM raw_events WITH NO DATA",
	}, queries)
}

func TestMaterializer_IsFullRefresh(t *testing.T) {
	t.Parallel()
	require.False(t, NewMaterializer(false).IsFullRefresh())
	require.True(t, NewMaterializer(true).IsFullRefresh())
}
