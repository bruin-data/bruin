package python

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_uvPythonRunner_ingestrLoaderFileFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		asset    *pipeline.Asset
		cmdArgs  []string
		expected []string
	}{
		{
			name: "should append loader file format when parameter exists",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"loader_file_format": "parquet",
				},
			},
			cmdArgs:  []string{"--existing", "arg"},
			expected: []string{"--existing", "arg", "--loader-file-format", "parquet"},
		},
		{
			name: "should not append loader file format when parameter is empty",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"loader_file_format": "",
				},
			},
			cmdArgs:  []string{"--existing", "arg"},
			expected: []string{"--existing", "arg"},
		},
		{
			name: "should not append loader file format when parameter doesn't exist",
			asset: &pipeline.Asset{
				Parameters: map[string]string{},
			},
			cmdArgs:  []string{"--existing", "arg"},
			expected: []string{"--existing", "arg"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := ConsolidatedParameters(context.Background(), nil, tt.asset, tt.cmdArgs)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddExtraPackages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		destURI       string
		sourceURI     string
		extraPackages []string
		want          []string
	}{
		{
			name:          "no mssql URIs",
			destURI:       "bigquery://project",
			sourceURI:     "snowflake://account",
			extraPackages: nil,
			want:          nil,
		},
		{
			name:          "mssql source URI",
			destURI:       "bigquery://project",
			sourceURI:     "mssql://server",
			extraPackages: nil,
			want:          []string{"pyodbc==5.1.0"},
		},
		{
			name:          "mssql destination URI",
			destURI:       "mssql://server",
			sourceURI:     "snowflake://account",
			extraPackages: nil,
			want:          []string{"pyodbc==5.1.0"},
		},
		{
			name:          "both mssql URIs",
			destURI:       "mssql://server1",
			sourceURI:     "mssql://server2",
			extraPackages: nil,
			want:          []string{"pyodbc==5.1.0"},
		},
		{
			name:          "existing extra packages with mssql",
			destURI:       "mssql://server",
			sourceURI:     "snowflake://account",
			extraPackages: []string{"existing-package==1.0.0"},
			want:          []string{"pyodbc==5.1.0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := AddExtraPackages(tt.destURI, tt.sourceURI, tt.extraPackages)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestConsolidatedParametersIntervals(t *testing.T) {
	t.Parallel()

	baseArgs := []string{"cmd"}

	tests := []struct {
		name     string
		setupCtx func() context.Context
		pipe     *pipeline.Pipeline
		asset    *pipeline.Asset
		expected []string
	}{
		{
			name: "full refresh uses asset start date",
			setupCtx: func() context.Context {
				ctx := context.Background()
				ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, true)
				return ctx
			},
			pipe: &pipeline.Pipeline{
				StartDate: "2023-12-30",
			},
			asset: &pipeline.Asset{
				StartDate: "2023-12-31",
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{Days: 2},
				},
			},
			expected: []string{
				"cmd",
				"--interval-start", "2023-12-31T00:00:00Z",
				"--interval-end", "2024-01-02T00:00:00Z",
				"--full-refresh",
			},
		},
		{
			name: "no full refresh applies modifiers",
			setupCtx: func() context.Context {
				ctx := context.Background()
				ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
				ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)
				ctx = context.WithValue(ctx, pipeline.RunConfigApplyIntervalModifiers, true)
				return ctx
			},
			pipe: &pipeline.Pipeline{},
			asset: &pipeline.Asset{
				IntervalModifiers: pipeline.IntervalModifiers{
					Start: pipeline.TimeModifier{Days: 1},
					End:   pipeline.TimeModifier{Days: -1},
				},
			},
			expected: []string{
				"cmd",
				"--interval-start", "2024-01-02T00:00:00Z",
				"--interval-end", "2024-01-01T00:00:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := tt.setupCtx()
			result, err := ConsolidatedParameters(ctx, tt.pipe, tt.asset, baseArgs)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
