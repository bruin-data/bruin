package python

import (
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
			result, err := ConsolidatedParameters(t.Context(), tt.asset, tt.cmdArgs, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestColumnHints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		columns        []pipeline.Column
		normalizeNames bool
		expected       string
	}{
		{
			name: "basic column hints without normalization",
			columns: []pipeline.Column{
				{Name: "id", Type: "integer"},
				{Name: "name", Type: "varchar"},
				{Name: "created_at", Type: "timestamp"},
			},
			normalizeNames: false,
			expected:       "id:bigint,name:text,created_at:timestamp",
		},
		{
			name: "column hints with name normalization",
			columns: []pipeline.Column{
				{Name: "DateOfBirth", Type: "date"},
				{Name: "User Name", Type: "string"},
			},
			normalizeNames: true,
			expected:       "date_of_birth:date,user_name:text",
		},
		{
			name:           "empty columns",
			columns:        []pipeline.Column{},
			normalizeNames: false,
			expected:       "",
		},
		{
			name: "unknown type is skipped",
			columns: []pipeline.Column{
				{Name: "id", Type: "integer"},
				{Name: "unknown_col", Type: "unknown_type"},
				{Name: "name", Type: "string"},
			},
			normalizeNames: false,
			expected:       "id:bigint,name:text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ColumnHints(tt.columns, tt.normalizeNames)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConsolidatedParameters_EnforceSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		asset      *pipeline.Asset
		columnOpts *ColumnHintOptions
		wantColumn bool
	}{
		{
			name: "enforce_schema=true adds columns",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"enforce_schema": "true",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "integer"},
				},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames:   false,
				EnforceSchemaByDefault: false,
			},
			wantColumn: true,
		},
		{
			name: "enforce_schema=false does not add columns",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"enforce_schema": "false",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "integer"},
				},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames:   false,
				EnforceSchemaByDefault: true,
			},
			wantColumn: false,
		},
		{
			name: "default enforces when EnforceSchemaByDefault=true",
			asset: &pipeline.Asset{
				Parameters: map[string]string{},
				Columns: []pipeline.Column{
					{Name: "id", Type: "integer"},
				},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames:   false,
				EnforceSchemaByDefault: true,
			},
			wantColumn: true,
		},
		{
			name: "default does not enforce when EnforceSchemaByDefault=false",
			asset: &pipeline.Asset{
				Parameters: map[string]string{},
				Columns: []pipeline.Column{
					{Name: "id", Type: "integer"},
				},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames:   false,
				EnforceSchemaByDefault: false,
			},
			wantColumn: false,
		},
		{
			name: "nil columnOpts does not add columns",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"enforce_schema": "true",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "integer"},
				},
			},
			columnOpts: nil,
			wantColumn: false,
		},
		{
			name: "normalizes column names when NormalizeColumnNames=true",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"enforce_schema": "true",
				},
				Columns: []pipeline.Column{
					{Name: "DateOfBirth", Type: "date"},
				},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames:   true,
				EnforceSchemaByDefault: false,
			},
			wantColumn: true,
		},
		{
			name: "seed asset with path parameter can disable enforce_schema",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"path":           "./seed.csv",
					"enforce_schema": "false",
				},
				Columns: []pipeline.Column{
					{Name: "name", Type: "varchar"},
					{Name: "contact_date", Type: "varchar"},
				},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames:   true,
				EnforceSchemaByDefault: true,
			},
			wantColumn: false,
		},
		{
			name: "seed asset without enforce_schema uses default (true)",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"path": "./seed.csv",
				},
				Columns: []pipeline.Column{
					{Name: "name", Type: "varchar"},
				},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames:   true,
				EnforceSchemaByDefault: true,
			},
			wantColumn: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := ConsolidatedParameters(t.Context(), tt.asset, []string{"--existing"}, tt.columnOpts)
			require.NoError(t, err)

			hasColumns := false
			for _, arg := range result {
				if arg == "--columns" {
					hasColumns = true
					break
				}
			}
			assert.Equal(t, tt.wantColumn, hasColumns, "expected columns presence to be %v", tt.wantColumn)
		})
	}
}

func TestColumnHints_Normalization(t *testing.T) {
	t.Parallel()

	columns := []pipeline.Column{
		{Name: "DateOfBirth", Type: "date"},
	}

	// Without normalization
	result := ColumnHints(columns, false)
	assert.Equal(t, "DateOfBirth:date", result)

	// With normalization
	result = ColumnHints(columns, true)
	assert.Equal(t, "date_of_birth:date", result)
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
