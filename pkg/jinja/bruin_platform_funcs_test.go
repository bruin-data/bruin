package jinja_test

import (
	"testing"

	_ "github.com/bruin-data/bruin/pkg/athena"
	_ "github.com/bruin-data/bruin/pkg/bigquery"
	_ "github.com/bruin-data/bruin/pkg/clickhouse"
	_ "github.com/bruin-data/bruin/pkg/databricks"
	_ "github.com/bruin-data/bruin/pkg/duckdb"
	_ "github.com/bruin-data/bruin/pkg/fabric"
	"github.com/bruin-data/bruin/pkg/jinja"
	_ "github.com/bruin-data/bruin/pkg/mssql"
	_ "github.com/bruin-data/bruin/pkg/mysql"
	_ "github.com/bruin-data/bruin/pkg/oracle"
	_ "github.com/bruin-data/bruin/pkg/postgres"
	_ "github.com/bruin-data/bruin/pkg/redshift"
	_ "github.com/bruin-data/bruin/pkg/snowflake"
	_ "github.com/bruin-data/bruin/pkg/spark"
	_ "github.com/bruin-data/bruin/pkg/synapse"
	_ "github.com/bruin-data/bruin/pkg/trino"
	_ "github.com/bruin-data/bruin/pkg/vertica"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlatformSpecificBuiltinSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		platform jinja.Platform
		query    string
		contains []string
		excludes []string
	}{
		{
			name:     "bigquery",
			platform: jinja.PlatformBigQuery,
			query:    `{{ bruin.get_url_host('page_url') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}||{{ bruin.generate_surrogate_key(['a', 'b']) }}`,
			contains: []string{"regexp_extract", "generate_array", "date_diff", "date_add", "to_hex(md5(", "concat("},
			excludes: []string{"generated_number <= 10000"},
		},
		{
			name:     "snowflake",
			platform: jinja.PlatformSnowflake,
			query:    `{{ bruin.get_url_path('page_url') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"split_part", "case when position('/'", "with recursive", "where to_date('2020-01-01') < to_date('2020-01-02')", "dateadd(day, (n + 1)"},
		},
		{
			name:     "postgres",
			platform: jinja.PlatformPostgres,
			query:    `{{ bruin.get_url_parameter('page_url', 'utm_source') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"split_part", "&utm_source=", "generate_series", "interval '1 day'"},
		},
		{
			name:     "redshift",
			platform: jinja.PlatformRedshift,
			query:    `{{ bruin.generate_surrogate_key(['a', 'b']) }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{" || ", "md5(", "where cast('2020-01-01' as timestamp) < cast('2020-01-02' as timestamp)", "dateadd(day, (n + 1)"},
			excludes: []string{"concat("},
		},
		{
			name:     "mysql",
			platform: jinja.PlatformMySQL,
			query:    `{{ bruin.get_url_host('page_url') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"substring_index", "with recursive", "where cast('2020-01-01' as date) < cast('2020-01-02' as date)", "DATE_ADD(cast('2020-01-01' as date), INTERVAL (n + 1) day)", "SET_VAR(cte_max_recursion_depth=1000000)"},
		},
		{
			name:     "duckdb",
			platform: jinja.PlatformDuckDB,
			query:    `{{ bruin.get_url_parameter('page_url', 'utm_source') }}||{{ bruin.get_url_path('page_url') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}||{{ bruin.generate_series(4) }}`,
			contains: []string{"concat('&'", "utm_source=", "replace(replace(replace(page_url, 'android-app://', '')", "generate_series", "cast(date_day as date) as date_day", "from generate_series(1, 4)"},
		},
		{
			name:     "databricks",
			platform: jinja.PlatformDatabricks,
			query:    `{{ bruin.get_url_parameter('page_url', 'utm_source') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"regexp_extract", "utm_source", ", 1)", "explode(", "sequence("},
		},
		{
			name:     "spark",
			platform: jinja.PlatformSpark,
			query:    `{{ bruin.get_url_parameter('page_url', 'utm_source') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"regexp_extract", "utm_source", ", 1)", "explode(", "sequence("},
		},
		{
			name:     "mssql",
			platform: jinja.PlatformMSSQL,
			query:    `{{ bruin.get_url_path('page_url') }}||{{ bruin.deduplicate('events', 'dateadd(day, 0, created_at)', 'updated_at desc') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}||{{ bruin.width_bucket('price', '0', '100', '10') }}`,
			contains: []string{"charindex", "select top (1) with ties *", "partition by dateadd(day, 0, created_at)", "option (maxrecursion 0)", "floor("},
			excludes: []string{"cross apply", "mod(", "ceil("},
		},
		{
			name:     "clickhouse",
			platform: jinja.PlatformClickhouse,
			query:    `{{ bruin.get_url_host('page_url') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"replaceRegexpOne", "extract(", "date_add(day", "numbers(greatest(dateDiff('day'"},
		},
		{
			name:     "athena",
			platform: jinja.PlatformAthena,
			query:    `{{ bruin.get_url_path('page_url') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"strpos", "substr", "date_add('day'", "date_diff('day'", "sequence(cast(0 as bigint)"},
		},
		{
			name:     "trino",
			platform: jinja.PlatformTrino,
			query:    `{{ bruin.get_url_path('page_url') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"strpos", "substr", "date_add('day'", "date_diff('day'", "sequence(cast(0 as bigint)"},
		},
		{
			name:     "synapse",
			platform: jinja.PlatformSynapse,
			query:    `{{ bruin.get_url_host('page_url') }}||{{ bruin.deduplicate('events', 'dateadd(day, 0, created_at)', 'updated_at desc') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}||{{ bruin.width_bucket('price', '0', '100', '10') }}`,
			contains: []string{"charindex", "select top (1) with ties *", "partition by dateadd(day, 0, created_at)", "with digits(n)", "cross join digits", "floor("},
			excludes: []string{"with recursive", "mod(", "ceil("},
		},
		{
			name:     "oracle",
			platform: jinja.PlatformOracle,
			query:    `{{ bruin.get_url_parameter('page_url', 'utm_source') }}||{{ bruin.generate_surrogate_key(['a', 'b']) }}||{{ bruin.deduplicate('events', 'user_id', 'updated_at desc') }}||{{ bruin.date_spine('hour', "timestamp '2020-01-01 00:00:00'", "timestamp '2020-01-01 02:00:00'") }}`,
			contains: []string{"regexp_substr", "standard_hash", " || ", "from events bruin_inner", "from events data", "cast(timestamp '2020-01-01 00:00:00' as date)", "NUMTODSINTERVAL", "connect by level"},
			excludes: []string{"concat(", "from events as"},
		},
		{
			name:     "fabric",
			platform: jinja.PlatformFabric,
			query:    `{{ bruin.get_url_host('page_url') }}||{{ bruin.deduplicate('events', 'dateadd(day, 0, created_at)', 'updated_at desc') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}||{{ bruin.width_bucket('price', '0', '100', '10') }}`,
			contains: []string{"charindex", "select top (1) with ties *", "partition by dateadd(day, 0, created_at)", "with digits(n)", "cross join digits", "floor("},
			excludes: []string{"with recursive", "mod(", "ceil("},
		},
		{
			name:     "vertica",
			platform: jinja.PlatformVertica,
			query:    `{{ bruin.get_url_path('page_url') }}||{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-02'") }}`,
			contains: []string{"split_part", "timestampadd(day, (n + 1)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := jinja.NewRenderer(jinja.Context{"bruin": jinja.BuiltinFunctions(tt.platform)})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			for _, expected := range tt.contains {
				assert.Contains(t, result, expected)
			}
			for _, unexpected := range tt.excludes {
				assert.NotContains(t, result, unexpected)
			}
		})
	}
}
