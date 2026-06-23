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
				Parameters: pipeline.ParameterMap{
					"loader_file_format": "parquet",
				},
			},
			cmdArgs:  []string{"--existing", "arg"},
			expected: []string{"--existing", "arg", "--loader-file-format", "parquet"},
		},
		{
			name: "should not append loader file format when parameter is empty",
			asset: &pipeline.Asset{
				Parameters: pipeline.ParameterMap{
					"loader_file_format": "",
				},
			},
			cmdArgs:  []string{"--existing", "arg"},
			expected: []string{"--existing", "arg"},
		},
		{
			name: "should not append loader file format when parameter doesn't exist",
			asset: &pipeline.Asset{
				Parameters: pipeline.ParameterMap{},
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

func TestConsolidatedParameters_IngestrFlagPassthrough(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Parameters: pipeline.ParameterMap{
			"incremental_key":      "updated_at",
			"incremental_strategy": "merge",
			"partition_by":         "event_date",
			"cluster_by":           "account_id",
			"schema_contract":      "freeze",
			"schema_naming":        "snake_case",
			"page_size":            "1000",
			"loader_file_size":     "2000",
			"extract_parallelism":  "7",
			"sql_reflection_level": "full",
			"sql_limit":            "500",
			"sql_exclude_columns":  "internal_notes",
			"mask":                 "email:hash",
			"pipelines_dir":        ".ingestr",
			"staging_bucket":       "gs://bucket/path",
			"staging_dataset":      "scratch",
			"flush_interval":       "10s",
			"flush_records":        "10000",
			"sql_backend":          "sqlalchemy",
			"loader_file_format":   "parquet",
			"no_inference":         "true",
			"trim_whitespace":      "true",
			"stream":               "true",
		},
	}

	result, err := ConsolidatedParameters(t.Context(), asset, []string{"--existing"}, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"--existing",
		"--incremental-key", "updated_at",
		"--incremental-strategy", "merge",
		"--partition-by", "event_date",
		"--cluster-by", "account_id",
		"--schema-contract", "freeze",
		"--schema-naming", "snake_case",
		"--page-size", "1000",
		"--loader-file-size", "2000",
		"--extract-parallelism", "7",
		"--sql-reflection-level", "full",
		"--sql-limit", "500",
		"--sql-exclude-columns", "internal_notes",
		"--mask", "email:hash",
		"--pipelines-dir", ".ingestr",
		"--staging-bucket", "gs://bucket/path",
		"--staging-dataset", "scratch",
		"--flush-interval", "10s",
		"--flush-records", "10000",
		"--sql-backend", "sqlalchemy",
		"--loader-file-format", "parquet",
		"--no-inference",
		"--trim-whitespace",
		"--stream",
	}, result)
}

func TestConsolidatedParameters_TrimWhitespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   pipeline.ParameterMap
		expected []string
	}{
		{
			name: "true appends trim whitespace flag",
			params: pipeline.ParameterMap{
				"trim_whitespace": "true",
			},
			expected: []string{"--existing", "--trim-whitespace"},
		},
		{
			name: "false does not append trim whitespace flag",
			params: pipeline.ParameterMap{
				"trim_whitespace": "false",
			},
			expected: []string{"--existing"},
		},
		{
			name:     "missing does not append trim whitespace flag",
			params:   pipeline.ParameterMap{},
			expected: []string{"--existing"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := ConsolidatedParameters(t.Context(), &pipeline.Asset{Parameters: tt.params}, []string{"--existing"}, nil)
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
			expected:       "id:int,name:text,created_at:timestamp",
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
			expected:       "id:int,name:text",
		},
		{
			name: "source_column adds dest:type:source triple",
			columns: []pipeline.Column{
				{Name: "id", SourceColumn: "sourceid", Type: "integer"},
				{Name: "email", SourceColumn: "eml", Type: "string"},
				{Name: "created_at", Type: "timestamp"},
			},
			normalizeNames: false,
			expected:       "id:int:sourceid,email:text:eml,created_at:timestamp",
		},
		{
			name: "source_column with name normalization normalizes dest name",
			columns: []pipeline.Column{
				{Name: "FirstName", SourceColumn: "fname", Type: "string"},
			},
			normalizeNames: true,
			expected:       "first_name:text:fname",
		},
		{
			name: "source_column without recognized type emits dest::source",
			columns: []pipeline.Column{
				{Name: "first_name", SourceColumn: "fname"},
				{Name: "email", SourceColumn: "eml", Type: "string"},
				{Name: "weird", SourceColumn: "wrd", Type: "unknown_type"},
			},
			normalizeNames: false,
			expected:       "first_name::fname,email:text:eml,weird::wrd",
		},
		{
			name: "integer widths are preserved, not collapsed to bigint",
			columns: []pipeline.Column{
				{Name: "tiny", Type: "TINYINT"},
				{Name: "small", Type: "SMALLINT"},
				{Name: "regular", Type: "INT"},
				{Name: "regular2", Type: "INTEGER"},
				{Name: "big", Type: "BIGINT"},
			},
			normalizeNames: false,
			expected:       "tiny:tinyint,small:smallint,regular:int,regular2:int,big:bigint",
		},
		{
			name: "platform integer aliases map to the right width",
			columns: []pipeline.Column{
				{Name: "pg_small", Type: "int2"},
				{Name: "pg_int", Type: "int4"},
				{Name: "pg_big", Type: "int8"},
				{Name: "ch_unsigned", Type: "uint16"},
				{Name: "spark_byte", Type: "byte"},
				{Name: "mysql_year", Type: "year"},
			},
			normalizeNames: false,
			expected:       "pg_small:smallint,pg_int:int,pg_big:bigint,ch_unsigned:int,spark_byte:tinyint,mysql_year:int",
		},
		{
			name: "no-tz timestamps map to timestamp_ntz while tz-aware stay timestamp",
			columns: []pipeline.Column{
				{Name: "naive", Type: "DATETIME2"},
				{Name: "naive_ntz", Type: "timestamp_ntz"},
				{Name: "small_dt", Type: "smalldatetime"},
				{Name: "aware", Type: "timestamp"},
				{Name: "aware_tz", Type: "timestamptz"},
			},
			normalizeNames: false,
			expected:       "naive:timestamp_ntz,naive_ntz:timestamp_ntz,small_dt:timestamp_ntz,aware:timestamp,aware_tz:timestamp",
		},
		{
			name: "interval and bigdecimal map to valid ingestr types",
			columns: []pipeline.Column{
				{Name: "span", Type: "interval"},
				{Name: "huge", Type: "bigdecimal"},
			},
			normalizeNames: false,
			expected:       "span:interval,huge:decimal",
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{},
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
				Parameters: pipeline.ParameterMap{},
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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
				Parameters: pipeline.ParameterMap{
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

func TestConsolidatedParameters_SourceColumn(t *testing.T) {
	t.Parallel()

	t.Run("source_column with enforce_schema=true emits dest:type:source", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Parameters: pipeline.ParameterMap{
				"enforce_schema": "true",
			},
			Columns: []pipeline.Column{
				{Name: "id", SourceColumn: "sourceid", Type: "integer"},
				{Name: "email", SourceColumn: "eml", Type: "string"},
			},
		}
		result, err := ConsolidatedParameters(t.Context(), asset, []string{"--existing"}, &ColumnHintOptions{})
		require.NoError(t, err)

		var columnsValue string
		for i, arg := range result {
			if arg == "--columns" && i+1 < len(result) {
				columnsValue = result[i+1]
				break
			}
		}
		assert.Equal(t, "id:int:sourceid,email:text:eml", columnsValue)
	})

	t.Run("source_column without enforce_schema does not emit --columns", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Parameters: pipeline.ParameterMap{},
			Columns: []pipeline.Column{
				{Name: "id", SourceColumn: "sourceid", Type: "integer"},
			},
		}
		result, err := ConsolidatedParameters(t.Context(), asset, []string{"--existing"}, &ColumnHintOptions{})
		require.NoError(t, err)
		for _, arg := range result {
			assert.NotEqual(t, "--columns", arg)
		}
	})
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
