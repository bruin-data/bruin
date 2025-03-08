package python

import (
	"context"
	"testing"

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
			result := ConsolidatedParameters(context.Background(), tt.asset, tt.cmdArgs)
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
