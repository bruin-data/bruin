package oracle

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/stretchr/testify/require"
)

func TestOracleGeneratedSQLParsesWithSQLGlot(t *testing.T) {
	t.Parallel()

	testBruinGeneratedSQLParsesWithSQLGlot(t, jinja.PlatformOracle, "oracle")
}

func testBruinGeneratedSQLParsesWithSQLGlot(t *testing.T, platform jinja.Platform, dialect string) {
	t.Helper()

	parser, err := sqlparser.NewSQLParserCached()
	require.NoError(t, err)
	require.NoError(t, parser.Start())

	t.Cleanup(func() {
		require.NoError(t, parser.Close())
	})

	for _, tt := range bruinGeneratedSQLParseCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			renderer := jinja.NewRenderer(jinja.Context{"bruin": jinja.BuiltinFunctions(platform)})
			rendered, err := renderer.Render(tt.template)
			require.NoErrorf(t, err, "failed to render generated SQL query %q", tt.name)

			isSingleSelect, err := parser.IsSingleSelectQuery(rendered, dialect)
			require.NoErrorf(t, err, "generated SQL query %q did not parse:\n%s", tt.name, rendered)
			require.Truef(t, isSingleSelect, "generated SQL query %q is not a single SELECT:\n%s", tt.name, rendered)
		})
	}
}

func bruinGeneratedSQLParseCases() []struct {
	name     string
	template string
} {
	return []struct {
		name     string
		template string
	}{
		{"group_by", `select user_id, account_id, count(*) as event_count from events {{ bruin.group_by(2) }}`},
		{"safe_divide", `select {{ bruin.safe_divide('revenue', 'sessions') }} as safe_divide from events`},
		{"safe_add", `select {{ bruin.safe_add(['revenue', 'tax']) }} as safe_add from events`},
		{"safe_add_three_fields", `select {{ bruin.safe_add(['revenue', 'tax', 'shipping']) }} as safe_add from events`},
		{"safe_subtract", `select {{ bruin.safe_subtract(['gross_revenue', 'discounts']) }} as safe_subtract from events`},
		{"safe_subtract_three_fields", `select {{ bruin.safe_subtract(['gross_revenue', 'discounts', 'refunds']) }} as safe_subtract from events`},
		{"surrogate_key", `select {{ bruin.generate_surrogate_key(['user_id', 'session_id']) }} as sk from events`},
		{"surrogate_key_single_field", `select {{ bruin.generate_surrogate_key(['user_id']) }} as sk from events`},
		{"pivot", `select user_id, {{ bruin.pivot('status', ['active', 'inactive'], quote_identifiers=false) }} from events group by user_id`},
		{"pivot_default_quoted_identifiers", `select user_id, {{ bruin.pivot('status', ['active', 'inactive']) }} from events group by user_id`},
		{"pivot_distinct_count", `select user_id, {{ bruin.pivot('status', ['active'], agg='count', distinct=true, prefix='status_', quote_identifiers=false) }} from events group by user_id`},
		{"pivot_no_alias", `select user_id, {{ bruin.pivot('status', ['active'], alias=false) }} from events group by user_id`},
		{"pivot_custom_comparison_values_suffix", `select user_id, {{ bruin.pivot('status', ['active'], cmp='!=', then_value='revenue', else_value='0', suffix='_excluded', quote_identifiers=false) }} from events group by user_id`},
		{"haversine_distance", `select {{ bruin.haversine_distance('lat1', 'lon1', 'lat2', 'lon2') }} as distance_miles from events`},
		{"haversine_distance_km", `select {{ bruin.haversine_distance('lat1', 'lon1', 'lat2', 'lon2', unit='km') }} as distance_km from events`},
		{"degrees_to_radians", `select {{ bruin.degrees_to_radians('angle_degrees') }} as radians_value from events`},
		{"width_bucket", `select {{ bruin.width_bucket('price', '0', '100', '10') }} as bucket_number from events`},
		{"slugify", `select 1 as {{ bruin.slugify('9 Active Users!') }} from events`},
		{"get_url_host", `select {{ bruin.get_url_host('page_url') }} as url_host from events`},
		{"get_url_path", `select {{ bruin.get_url_path('page_url') }} as url_path from events`},
		{"get_url_parameter", `select {{ bruin.get_url_parameter('page_url', 'utm_source') }} as utm_source from events`},
		{"generate_series", `{{ bruin.generate_series(17) }}`},
		{"generate_series_one", `{{ bruin.generate_series(1) }}`},
		{"deduplicate", `{{ bruin.deduplicate('events', 'user_id, account_id', 'updated_at desc') }}`},
		{"deduplicate_expression_partition", `{{ bruin.deduplicate('events', 'cast(created_at as date), account_id', 'updated_at desc, event_id') }}`},
		{"date_spine_day", `{{ bruin.date_spine('day', "'2020-01-01'", "'2020-01-05'") }}`},
		{"date_spine_week", `{{ bruin.date_spine('week', "'2020-01-01'", "'2020-02-01'") }}`},
		{"date_spine_month_non_aligned", `{{ bruin.date_spine('month', "'2020-01-15'", "'2020-04-01'") }}`},
		{"date_spine_quarter", `{{ bruin.date_spine('quarter', "'2020-01-15'", "'2020-10-01'") }}`},
		{"date_spine_year", `{{ bruin.date_spine('year', "'2020-01-15'", "'2023-01-01'") }}`},
		{"date_spine_hour", `{{ bruin.date_spine('hour', "'2020-01-01 00:00:00'", "'2020-01-01 03:00:00'") }}`},
		{"date_spine_minute", `{{ bruin.date_spine('minute', "'2020-01-01 00:00:00'", "'2020-01-01 00:03:00'") }}`},
		{"date_spine_second", `{{ bruin.date_spine('second', "'2020-01-01 00:00:00'", "'2020-01-01 00:00:03'") }}`},
		{"date_spine_millisecond", `{{ bruin.date_spine('millisecond', "'2020-01-01 00:00:00.000'", "'2020-01-01 00:00:00.003'") }}`},
		{"date_spine_microsecond", `{{ bruin.date_spine('microsecond', "'2020-01-01 00:00:00.000000'", "'2020-01-01 00:00:00.000003'") }}`},
	}
}
