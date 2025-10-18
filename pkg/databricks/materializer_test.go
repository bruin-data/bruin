package databricks

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bruin-data/bruin/pkg/pipeline"
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
