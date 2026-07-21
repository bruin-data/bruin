package python

import (
	"context"
	"fmt"
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
			"incremental_key":            "updated_at",
			"incremental_strategy":       "merge",
			"partition_by":               "event_date",
			"cluster_by":                 "account_id",
			"schema_contract":            "freeze",
			"schema_naming":              "snake_case",
			"page_size":                  "1000",
			"loader_file_size":           "2000",
			"extract_parallelism":        "7",
			"extract_partition_by":       "event_time",
			"extract_partition_interval": "6h",
			"sql_reflection_level":       "full",
			"sql_limit":                  "500",
			"sql_exclude_columns":        "internal_notes",
			"mask":                       "email:hash",
			"pipelines_dir":              ".ingestr",
			"staging_bucket":             "gs://bucket/path",
			"staging_dataset":            "scratch",
			"flush_interval":             "10s",
			"flush_records":              "10000",
			"sql_backend":                "sqlalchemy",
			"loader_file_format":         "parquet",
			"no_inference":               "true",
			"trim_whitespace":            "true",
			"stream":                     "true",
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
		"--extract-partition-by", "event_time",
		"--extract-partition-interval", "6h",
		"--sql-reflection-level", "full",
		"--sql-limit", "500",
		"--sql-exclude-columns", "internal_notes",
		"--pipelines-dir", ".ingestr",
		"--staging-bucket", "gs://bucket/path",
		"--staging-dataset", "scratch",
		"--sql-backend", "sqlalchemy",
		"--loader-file-format", "parquet",
		"--no-inference",
		"--trim-whitespace",
		"--stream",
		"--flush-interval", "10s",
		"--flush-records", "10000",
		"--mask", "email:hash",
	}, result)
}

func TestConsolidatedParameters_StreamGating(t *testing.T) {
	t.Parallel()

	t.Run("flush flags are dropped without stream", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Parameters: pipeline.ParameterMap{
				"flush_interval": "10s",
				"flush_records":  "10000",
			},
		}
		result, err := ConsolidatedParameters(t.Context(), asset, []string{"--existing"}, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"--existing"}, result)
	})

	t.Run("flush flags are emitted with stream", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Parameters: pipeline.ParameterMap{
				"stream":         "true",
				"flush_interval": "10s",
				"flush_records":  "10000",
			},
		}
		result, err := ConsolidatedParameters(t.Context(), asset, []string{"--existing"}, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"--existing", "--stream", "--flush-interval", "10s", "--flush-records", "10000"}, result)
	})

	t.Run("interval-end is suppressed for a streaming asset", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Parameters: pipeline.ParameterMap{"stream": "true"}}
		ctx := context.WithValue(t.Context(), pipeline.RunConfigStartDate, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
		result, err := ConsolidatedParameters(ctx, asset, []string{"--existing"}, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"--existing", "--stream", "--interval-start", "2025-01-01T00:00:00Z"}, result)
		assert.NotContains(t, result, "--interval-end")
	})

	t.Run("interval-end is kept for a non-streaming asset", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{Parameters: pipeline.ParameterMap{}}
		ctx := context.WithValue(t.Context(), pipeline.RunConfigStartDate, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
		result, err := ConsolidatedParameters(ctx, asset, []string{"--existing"}, nil)
		require.NoError(t, err)
		assert.Contains(t, result, "--interval-end")
	})
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

func TestConsolidatedParameters_ColumnMasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		params     pipeline.ParameterMap
		columns    []pipeline.Column
		columnOpts *ColumnHintOptions
		expected   []string
	}{
		{
			name: "column mask method emits column-qualified mask flag",
			columns: []pipeline.Column{
				{Name: "email", Mask: "hash"},
				{Name: "phone", Mask: "sha256"},
			},
			expected: []string{"--existing", "--mask", "email:hash", "--mask", "phone:sha256"},
		},
		{
			name: "full mask rule is preserved",
			columns: []pipeline.Column{
				{Name: "email", Mask: "contact_email:hash"},
			},
			expected: []string{"--existing", "--mask", "contact_email:hash"},
		},
		{
			name: "column names are normalized when requested",
			columns: []pipeline.Column{
				{Name: "Contact Email", Mask: "hash"},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames: true,
			},
			expected: []string{"--existing", "--mask", "contact_email:hash"},
		},
		{
			name: "asset mask is preserved and duplicate column mask is skipped",
			params: pipeline.ParameterMap{
				"mask": "email:hash",
			},
			columns: []pipeline.Column{
				{Name: "email", Mask: "hash"},
				{Name: "phone", Mask: "sha256"},
			},
			expected: []string{"--existing", "--mask", "email:hash", "--mask", "phone:sha256"},
		},
		{
			name: "empty masks are ignored",
			columns: []pipeline.Column{
				{Name: "email", Mask: " "},
			},
			expected: []string{"--existing"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := tt.params
			if params == nil {
				params = pipeline.ParameterMap{}
			}
			result, err := ConsolidatedParameters(t.Context(), &pipeline.Asset{
				Parameters: params,
				Columns:    tt.columns,
			}, []string{"--existing"}, tt.columnOpts)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func intPtr(i int) *int { return &i }

func TestColumnHints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		columns        []pipeline.Column
		normalizeNames bool
		overlay        map[string]string
		wrappers       map[string]bool
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
				{Name: "spark_byte", Type: "byte"},
				{Name: "mysql_year", Type: "year"},
			},
			normalizeNames: false,
			expected:       "pg_small:smallint,pg_int:int,pg_big:bigint,spark_byte:tinyint,mysql_year:int",
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
		{
			name: "inline sized string aliases become text(n)",
			columns: []pipeline.Column{
				{Name: "a", Type: "string(50)"},
				{Name: "b", Type: "varchar(100)"},
				{Name: "c", Type: "nvarchar(50)"},
				{Name: "d", Type: "text(255)"},
				{Name: "e", Type: "longtext(20)"},
			},
			normalizeNames: false,
			expected:       "a:text(50),b:text(100),c:text(50),d:text(255),e:text(20)",
		},
		{
			name: "length field sizes string types",
			columns: []pipeline.Column{
				{Name: "a", Type: "varchar", Length: intPtr(100)},
				{Name: "b", Type: "nvarchar", Length: intPtr(50)},
				{Name: "c", Type: "text", Length: intPtr(255)},
			},
			normalizeNames: false,
			expected:       "a:text(100),b:text(50),c:text(255)",
		},
		{
			name: "inline length takes precedence over length field",
			columns: []pipeline.Column{
				{Name: "a", Type: "varchar(100)", Length: intPtr(50)},
			},
			normalizeNames: false,
			expected:       "a:text(100)",
		},
		{
			name: "unbounded and non-positive lengths stay unbounded text",
			columns: []pipeline.Column{
				{Name: "a", Type: "varchar"},
				{Name: "b", Type: "varchar(max)"},
				{Name: "c", Type: "varchar", Length: intPtr(0)},
			},
			normalizeNames: false,
			expected:       "a:text,b:text,c:text",
		},
		{
			name: "non-string parameterized types use base mapping without length",
			columns: []pipeline.Column{
				{Name: "kept", Type: "int"},
				{Name: "sized_int", Type: "int(11)"},
				{Name: "sized_dec", Type: "decimal(10,2)"},
			},
			normalizeNames: false,
			expected:       "kept:int,sized_int:int,sized_dec:decimal",
		},
		{
			name: "sized string with source_column emits dest:text(n):source",
			columns: []pipeline.Column{
				{Name: "email", SourceColumn: "eml", Type: "varchar(255)"},
			},
			normalizeNames: false,
			expected:       "email:text(255):eml",
		},
		{
			name: "nvarchar maps to text and honors length",
			columns: []pipeline.Column{
				{Name: "bare", Type: "nvarchar"},
				{Name: "inline", Type: "nvarchar(100)"},
				{Name: "field", Type: "nvarchar", Length: intPtr(50)},
				{Name: "renamed", SourceColumn: "src", Type: "nvarchar(255)"},
			},
			normalizeNames: false,
			expected:       "bare:text,inline:text(100),field:text(50),renamed:text(255):src",
		},
		{
			name: "destination overlay aliases are applied",
			columns: []pipeline.Column{
				{Name: "id", Type: "uint64"},
				{Name: "ts", Type: "DateTime64"},
				{Name: "ts_prec", Type: "DateTime64(3)"},
				{Name: "name", Type: "string"},
			},
			normalizeNames: false,
			overlay: map[string]string{
				"uint64":     "bigint",
				"datetime64": "timestamp",
			},
			expected: "id:bigint,ts:timestamp,ts_prec:timestamp,name:text",
		},
		{
			name: "nullable and lowcardinality wrappers peel to inner type",
			columns: []pipeline.Column{
				{Name: "a", Type: "Nullable(integer)"},
				{Name: "b", Type: "LowCardinality(varchar(50))"},
				{Name: "c", Type: "Nullable(LowCardinality(timestamp))"},
			},
			normalizeNames: false,
			wrappers: map[string]bool{
				"nullable":       true,
				"lowcardinality": true,
			},
			expected: "a:int,b:text(50),c:timestamp",
		},
		{
			name: "wrappers are not peeled without destination wrappers",
			columns: []pipeline.Column{
				{Name: "a", Type: "Nullable(integer)"},
				{Name: "b", Type: "string"},
			},
			normalizeNames: false,
			expected:       "b:text",
		},
		{
			name: "destination overlay wins over defaults",
			columns: []pipeline.Column{
				{Name: "x", Type: "integer"},
			},
			normalizeNames: false,
			overlay:        map[string]string{"integer": "bigint"},
			expected:       "x:bigint",
		},
		{
			name: "clickhouse-only types are skipped without overlay",
			columns: []pipeline.Column{
				{Name: "id", Type: "uint64"},
				{Name: "ts", Type: "datetime64"},
				{Name: "name", Type: "string"},
			},
			normalizeNames: false,
			expected:       "name:text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ColumnHints(tt.columns, tt.normalizeNames, tt.overlay, tt.wrappers)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestColumnHints_SizedTypes guards that every alias mapping to a sized ingestr type
// accepts a length, both via an inline length and via the length field.
func TestColumnHints_SizedTypes(t *testing.T) {
	t.Parallel()

	sized := make(map[string]string) // source alias -> expected hint
	for typ, hint := range TypeHintMapping {
		if ingestrSizedTypes[hint] {
			sized[typ] = hint
		}
	}
	require.NotEmpty(t, sized)

	for typ, hint := range sized {
		want := fmt.Sprintf("c:%s(50)", hint)
		t.Run(typ, func(t *testing.T) {
			t.Parallel()
			inline := ColumnHints([]pipeline.Column{{Name: "c", Type: typ + "(50)"}}, false, nil, nil)
			assert.Equal(t, want, inline, "inline length for %q", typ)

			field := ColumnHints([]pipeline.Column{{Name: "c", Type: typ, Length: intPtr(50)}}, false, nil, nil)
			assert.Equal(t, want, field, "length field for %q", typ)
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
			name: "type hint overlay is applied when enforcing schema",
			asset: &pipeline.Asset{
				Parameters: pipeline.ParameterMap{},
				Columns: []pipeline.Column{
					{Name: "id", Type: "uint64"},
					{Name: "ts", Type: "datetime64"},
				},
			},
			columnOpts: &ColumnHintOptions{
				NormalizeColumnNames:   false,
				EnforceSchemaByDefault: true,
				TypeHintOverlay: map[string]string{
					"uint64":     "bigint",
					"datetime64": "timestamp",
				},
			},
			wantColumn: true,
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
	result := ColumnHints(columns, false, nil, nil)
	assert.Equal(t, "DateOfBirth:date", result)

	// With normalization
	result = ColumnHints(columns, true, nil, nil)
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
