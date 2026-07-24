package jinja

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuiltin_GroupBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "group_by 1",
			query: "select col1, count(*) from table1 {{ bruin.group_by(1) }}",
			want:  "select col1, count(*) from table1 group by 1",
		},
		{
			name:  "group_by 3",
			query: "select col1, col2, col3, count(*) from table1 {{ bruin.group_by(3) }}",
			want:  "select col1, col2, col3, count(*) from table1 group by 1, 2, 3",
		},
		{
			name:  "group_by 5",
			query: "{{ bruin.group_by(5) }}",
			want:  "group by 1, 2, 3, 4, 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBuiltin_SafeDivide(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "basic safe_divide",
			query: "select {{ bruin.safe_divide('revenue', 'sessions') }}",
			want:  "select (revenue) / nullif((sessions), 0)",
		},
		{
			name:  "safe_divide with expressions",
			query: "select {{ bruin.safe_divide('sum(revenue)', 'count(distinct user_id)') }}",
			want:  "select (sum(revenue)) / nullif((count(distinct user_id)), 0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBuiltin_SafeAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "safe_add with list",
			query: "select {{ bruin.safe_add(['col1', 'col2', 'col3']) }} as total",
			want:  "select coalesce(col1, 0) +\n    coalesce(col2, 0) +\n    coalesce(col3, 0) as total",
		},
		{
			name:  "safe_add with two columns",
			query: "{{ bruin.safe_add(['revenue', 'tax']) }}",
			want:  "coalesce(revenue, 0) +\n    coalesce(tax, 0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBuiltin_SafeSubtract(t *testing.T) {
	t.Parallel()

	renderer := NewRenderer(Context{})
	result, err := renderer.Render("{{ bruin.safe_subtract(['revenue', 'cost', 'tax']) }}")
	require.NoError(t, err)
	assert.Equal(t, "coalesce(revenue, 0) -\n    coalesce(cost, 0) -\n    coalesce(tax, 0)", result)
}

func TestBuiltin_GenerateSurrogateKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "single field",
			query: "{{ bruin.generate_surrogate_key(['user_id']) }}",
			want:  "md5(coalesce(cast(user_id as varchar), '_bruin_surrogate_key_null_'))",
		},
		{
			name:  "multiple fields",
			query: "{{ bruin.generate_surrogate_key(['user_id', 'session_id']) }}",
			want:  "md5(concat(coalesce(cast(user_id as varchar), '_bruin_surrogate_key_null_'), '-', coalesce(cast(session_id as varchar), '_bruin_surrogate_key_null_')))",
		},
		{
			name:  "three fields",
			query: "{{ bruin.generate_surrogate_key(['a', 'b', 'c']) }}",
			want:  "md5(concat(coalesce(cast(a as varchar), '_bruin_surrogate_key_null_'), '-', coalesce(cast(b as varchar), '_bruin_surrogate_key_null_'), '-', coalesce(cast(c as varchar), '_bruin_surrogate_key_null_')))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBuiltin_Pivot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		{
			name:  "basic pivot",
			query: "{{ bruin.pivot('status', ['active', 'churned']) }}",
			contains: []string{
				"sum(",
				"when status = 'active'",
				"when status = 'churned'",
				`as "active"`,
				`as "churned"`,
			},
		},
		{
			name:  "pivot with count agg",
			query: "{{ bruin.pivot('color', ['red', 'blue'], agg='count') }}",
			contains: []string{
				"count(",
				"when color = 'red'",
				"when color = 'blue'",
			},
		},
		{
			name:  "pivot with prefix and suffix",
			query: "{{ bruin.pivot('status', ['active'], prefix='is_', suffix='_flag') }}",
			contains: []string{
				`as "is_active_flag"`,
			},
		},
		{
			name:  "pivot with distinct",
			query: "{{ bruin.pivot('type', ['a'], distinct=true) }}",
			contains: []string{
				"distinct case",
			},
		},
		{
			name:  "pivot without alias",
			query: "{{ bruin.pivot('type', ['a'], alias=false) }}",
			contains: []string{
				"sum(",
			},
		},
		{
			name:  "pivot without quoting",
			query: "{{ bruin.pivot('type', ['My Value'], quote_identifiers=false) }}",
			contains: []string{
				"as my_value",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			for _, substr := range tt.contains {
				assert.Contains(t, result, substr, "expected output to contain %q", substr)
			}
		})
	}
}

func TestBuiltin_Pivot_NoAlias(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render("{{ bruin.pivot('type', ['a'], alias=false) }}")
	require.NoError(t, err)
	assert.NotContains(t, result, `as "`)
}

func TestBuiltin_HaversineDistance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		{
			name:  "default miles",
			query: "{{ bruin.haversine_distance('lat1', 'lon1', 'lat2', 'lon2') }}",
			contains: []string{
				"asin(sqrt(power",
				"lat2 - lat1",
				"lon2 - lon1",
				"* 1",
			},
		},
		{
			name:  "kilometers",
			query: "{{ bruin.haversine_distance('lat1', 'lon1', 'lat2', 'lon2', unit='km') }}",
			contains: []string{
				"* 1.60934",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			for _, substr := range tt.contains {
				assert.Contains(t, result, substr)
			}
		})
	}
}

func TestBuiltin_HaversineDistanceRejectsUnknownUnit(t *testing.T) {
	t.Parallel()

	renderer := NewRenderer(Context{})
	_, err := renderer.Render("{{ bruin.haversine_distance('lat1', 'lon1', 'lat2', 'lon2', unit='meters') }}")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "haversine_distance unit must be 'mi' or 'km'")
}

func TestBuiltin_DegreesToRadians(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render("{{ bruin.degrees_to_radians('angle_col') }}")
	require.NoError(t, err)
	assert.Equal(t, "acos(-1) * angle_col / 180", result)
}

func TestBuiltin_WidthBucket(t *testing.T) {
	t.Parallel()

	t.Run("zero min", func(t *testing.T) {
		t.Parallel()
		renderer := NewRenderer(Context{})
		result, err := renderer.Render("{{ bruin.width_bucket('price', '0', '100', '10') }}")
		require.NoError(t, err)
		assert.Contains(t, result, "mod(")
		assert.Contains(t, result, "then 0")
		assert.Contains(t, result, "ceil(")
		assert.Contains(t, result, "price")
		assert.Contains(t, result, "cast(10 as numeric) + 1")
	})

	t.Run("non-zero min offsets mod correctly", func(t *testing.T) {
		t.Parallel()
		renderer := NewRenderer(Context{})
		result, err := renderer.Render("{{ bruin.width_bucket('val', '3', '23', '4') }}")
		require.NoError(t, err)
		// The mod must subtract minValue so boundary detection is relative to the range start.
		assert.Contains(t, result, "cast(val as numeric) - cast(3 as numeric)")
	})
}

func TestBuiltin_Deduplicate(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render("{{ bruin.deduplicate('my_table', 'user_id', 'updated_at desc') }}")
	require.NoError(t, err)
	assert.Contains(t, result, "row_number() over (")
	assert.Contains(t, result, "partition by user_id")
	assert.Contains(t, result, "order by updated_at desc")
	assert.Contains(t, result, "from my_table as _inner")
	assert.Contains(t, result, "natural join row_numbered")
	assert.Contains(t, result, "where row_numbered.__bruin_row_number = 1")
}

func TestBuiltin_GenerateSeries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		contains []string
	}{
		{
			name:  "small series",
			query: "{{ bruin.generate_series(4) }}",
			contains: []string{
				"select 0 as generated_number union all select 1",
				"p0.generated_number * power(2, 0)",
				"p1.generated_number * power(2, 1)",
				"p as p0",
				"cross join",
				"p as p1",
				"where generated_number <= 4",
			},
		},
		{
			name:  "series of 1",
			query: "{{ bruin.generate_series(1) }}",
			contains: []string{
				"p0.generated_number * power(2, 0)",
				"where generated_number <= 1",
			},
		},
		{
			name:  "larger series",
			query: "{{ bruin.generate_series(100) }}",
			contains: []string{
				"p6.generated_number * power(2, 6)",
				"where generated_number <= 100",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			for _, substr := range tt.contains {
				assert.Contains(t, result, substr, "expected output to contain %q", substr)
			}
		})
	}
}

func TestBuiltin_DateSpine(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render(`{{ bruin.date_spine('day', "'2020-01-01'", "'2025-01-01'") }}`)
	require.NoError(t, err)
	assert.Contains(t, result, "dateadd(")
	assert.Contains(t, result, "day,")
	assert.Contains(t, result, "'2020-01-01'")
	assert.Contains(t, result, "as date_day")
	assert.Contains(t, result, "dateadd(day, (n + 1), '2020-01-01')")
	assert.Contains(t, result, "where dateadd(day, (n + 1), '2020-01-01') < '2025-01-01'")
	assert.Contains(t, result, "select date_day")
	assert.Contains(t, result, "with recursive")
	assert.NotContains(t, result, "generated_number <= 10000")
}

func TestBuiltin_DateSpine_Month(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render(`{{ bruin.date_spine('month', "'2020-01-01'", "'2025-01-01'") }}`)
	require.NoError(t, err)
	assert.Contains(t, result, "as date_month")
	assert.Contains(t, result, "where dateadd(month, (n + 1), '2020-01-01') < '2025-01-01'")
}

func TestBuiltin_Slugify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "basic slugify",
			query: "{{ bruin.slugify('Hello World') }}",
			want:  "hello_world",
		},
		{
			name:  "special characters",
			query: "{{ bruin.slugify('My Column Name!@#') }}",
			want:  "my_column_name",
		},
		{
			name:  "leading digit",
			query: "{{ bruin.slugify('1st_place') }}",
			want:  "_1st_place",
		},
		{
			name:  "dashes to underscores",
			query: "{{ bruin.slugify('some-value-here') }}",
			want:  "some_value_here",
		},
		{
			name:  "empty string",
			query: "{{ bruin.slugify('') }}",
			want:  "",
		},
		{
			name:  "already valid",
			query: "{{ bruin.slugify('valid_name') }}",
			want:  "valid_name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewRenderer(Context{})
			result, err := renderer.Render(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBuiltin_GetURLHost(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render("{{ bruin.get_url_host('page_url') }}")
	require.NoError(t, err)
	assert.Contains(t, result, "split_part")
	assert.Contains(t, result, "replace")
	assert.Contains(t, result, "page_url")
	assert.Contains(t, result, "'http://'")
	assert.Contains(t, result, "'https://'")
}

func TestBuiltin_GetURLParameter(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render("{{ bruin.get_url_parameter('page_url', 'utm_source') }}")
	require.NoError(t, err)
	assert.Contains(t, result, "split_part")
	assert.Contains(t, result, "'&utm_source='")
	assert.Contains(t, result, "'&'")
	assert.Contains(t, result, "nullif")
}

func TestBuiltin_GetURLPath(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render("{{ bruin.get_url_path('page_url') }}")
	require.NoError(t, err)
	assert.Contains(t, result, "replace")
	assert.Contains(t, result, "page_url")
	assert.Contains(t, result, "cast(")
	assert.Contains(t, result, "as varchar)")
}

func TestBuiltin_FunctionsAvailableByDefault(t *testing.T) {
	t.Parallel()

	// Ensure bruin functions are available in all renderer types.
	t.Run("NewRenderer", func(t *testing.T) {
		t.Parallel()
		renderer := NewRenderer(Context{})
		result, err := renderer.Render("{{ bruin.group_by(2) }}")
		require.NoError(t, err)
		assert.Equal(t, "group by 1, 2", result)
	})

	t.Run("NewRendererWithMacros", func(t *testing.T) {
		t.Parallel()
		renderer := NewRendererWithMacros(Context{}, "")
		result, err := renderer.Render("{{ bruin.group_by(2) }}")
		require.NoError(t, err)
		assert.Equal(t, "group by 1, 2", result)
	})
}

func TestBuiltin_CombinedUsage(t *testing.T) {
	t.Parallel()

	query := `select
    {{ bruin.generate_surrogate_key(['user_id', 'event_date']) }} as surrogate_key,
    user_id,
    event_date,
    {{ bruin.safe_divide('revenue', 'sessions') }} as revenue_per_session
from events
{{ bruin.group_by(3) }}`

	renderer := NewRenderer(Context{})
	result, err := renderer.Render(query)
	require.NoError(t, err)
	assert.Contains(t, result, "md5(concat(")
	assert.Contains(t, result, "nullif(")
	assert.Contains(t, result, "group by 1, 2, 3")
}

func TestBuiltin_PivotEscapesSingleQuotes(t *testing.T) {
	t.Parallel()
	renderer := NewRenderer(Context{})
	result, err := renderer.Render(`{{ bruin.pivot('name', ["it's"]) }}`)
	require.NoError(t, err)
	assert.Contains(t, result, "it''s")
}

// Test helper functions directly.
func TestGetPowersOfTwo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    int
		expected int
	}{
		{1, 1},
		{2, 1},
		{3, 2},
		{4, 2},
		{5, 3},
		{8, 3},
		{9, 4},
		{16, 4},
		{100, 7},
		{1000, 10},
		{10000, 14},
	}

	for _, tt := range tests {
		t.Run(strings.Repeat("x", tt.input), func(t *testing.T) {
			t.Parallel()
			result := getPowersOfTwo(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ---------------------------------------------------------------------------
// Builder and variant function tests
// ---------------------------------------------------------------------------

// rendererWithOverrides creates a renderer that merges the given overrides on top of defaults.
func rendererWithOverrides(overrides map[string]any) *Renderer {
	funcs := BuiltinFunctions()
	for k, v := range overrides {
		funcs[k] = v
	}
	return NewRenderer(Context{"bruin": funcs})
}

func TestSurrogateKeyWith(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		castType string
		hashFn   func(string) string
		contains []string
	}{
		{
			name:     "string cast with to_hex",
			castType: "string",
			hashFn:   func(e string) string { return "to_hex(md5(" + e + "))" },
			contains: []string{"to_hex(md5(", "cast(user_id as string)"},
		},
		{
			name:     "varchar cast with hashbytes",
			castType: "varchar",
			hashFn:   func(e string) string { return "convert(varchar(32), hashbytes('md5', " + e + "), 2)" },
			contains: []string{"hashbytes('md5',", "cast(user_id as varchar)"},
		},
		{
			name:     "char cast with md5",
			castType: "char",
			hashFn:   func(e string) string { return "md5(" + e + ")" },
			contains: []string{"md5(coalesce(", "cast(user_id as char)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := rendererWithOverrides(map[string]any{
				"generate_surrogate_key": SurrogateKeyWith(tt.castType, tt.hashFn),
			})
			result, err := renderer.Render("{{ bruin.generate_surrogate_key(['user_id']) }}")
			require.NoError(t, err)
			for _, substr := range tt.contains {
				assert.Contains(t, result, substr, "expected output to contain %q", substr)
			}
		})
	}
}

func TestDeduplicateOverrideViaRegistry(t *testing.T) {
	t.Parallel()

	// Test that a custom deduplicate function can be registered and used via the override mechanism.
	customDeduplicate := func(relation, partitionBy, orderBy string) string {
		return "select * from " + relation + " qualify row_number() over (partition by " + partitionBy + " order by " + orderBy + ") = 1"
	}

	renderer := rendererWithOverrides(map[string]any{
		"deduplicate": customDeduplicate,
	})
	result, err := renderer.Render("{{ bruin.deduplicate('my_table', 'user_id', 'updated_at desc') }}")
	require.NoError(t, err)
	assert.Contains(t, result, "qualify")
	assert.Contains(t, result, "partition by user_id")
}

func TestHaversineDistanceWithRadians(t *testing.T) {
	t.Parallel()
	renderer := rendererWithOverrides(map[string]any{
		"haversine_distance": HaversineDistanceWithRadians(func(expr string) string {
			return "(" + expr + ") * acos(-1) / 180"
		}),
	})
	result, err := renderer.Render("{{ bruin.haversine_distance('lat1', 'lon1', 'lat2', 'lon2') }}")
	require.NoError(t, err)
	assert.Contains(t, result, "acos(-1) / 180")
	assert.NotContains(t, result, "radians(")
}

func TestDateSpineWithDateAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dateAddFn func(string, string, string) string
		contains  []string
	}{
		{
			name: "DATE_ADD INTERVAL",
			dateAddFn: func(dp, n, s string) string {
				return "DATE_ADD(" + s + ", INTERVAL " + n + " " + dp + ")"
			},
			contains: []string{"DATE_ADD(", "INTERVAL", "day)"},
		},
		{
			name: "interval addition",
			dateAddFn: func(dp, n, s string) string {
				return "(" + s + " + " + n + " * INTERVAL '1 " + dp + "')"
			},
			contains: []string{"* INTERVAL '1 day'"},
		},
		{
			name: "quoted datepart",
			dateAddFn: func(dp, n, s string) string {
				return "date_add('" + dp + "', " + n + ", " + s + ")"
			},
			contains: []string{"date_add('day',"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renderer := rendererWithOverrides(map[string]any{
				"date_spine": DateSpineWithDateAdd(tt.dateAddFn),
			})
			result, err := renderer.Render(`{{ bruin.date_spine('day', "'2020-01-01'", "'2025-01-01'") }}`)
			require.NoError(t, err)
			for _, substr := range tt.contains {
				assert.Contains(t, result, substr, "expected output to contain %q", substr)
			}
		})
	}
}

func TestRegisterPlatformOverrides(t *testing.T) {
	t.Parallel()

	// Register overrides for a test-only platform using an inline function.
	testPlatform := Platform("__test_platform__")
	RegisterPlatformOverrides(testPlatform, map[string]any{
		"deduplicate": func(relation, partitionBy, orderBy string) string {
			return "select * from " + relation + " qualify row_number() over (partition by " + partitionBy + " order by " + orderBy + ") = 1"
		},
	})

	renderer := NewRenderer(Context{
		"bruin": BuiltinFunctions(testPlatform),
	})
	result, err := renderer.Render("{{ bruin.deduplicate('t', 'id', 'ts desc') }}")
	require.NoError(t, err)
	assert.Contains(t, result, "qualify")
	assert.NotContains(t, result, "natural join")
}

func TestPlatformForAssetType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assetType pipeline.AssetType
		expected  Platform
	}{
		{pipeline.AssetTypeBigqueryQuery, PlatformBigQuery},
		{pipeline.AssetTypeSnowflakeQuery, PlatformSnowflake},
		{pipeline.AssetTypePostgresQuery, PlatformPostgres},
		{pipeline.AssetTypeRedshiftQuery, PlatformRedshift},
		{pipeline.AssetTypeMsSQLQuery, PlatformMSSQL},
		{pipeline.AssetTypeDuckDBQuery, PlatformDuckDB},
		{pipeline.AssetTypeMotherduckQuery, PlatformDuckDB},
		{pipeline.AssetTypeDatabricksQuery, PlatformDatabricks},
		{pipeline.AssetTypeSparkQuery, PlatformSpark},
		{pipeline.AssetTypeAthenaQuery, PlatformAthena},
		{pipeline.AssetTypeTrinoQuery, PlatformTrino},
		{pipeline.AssetTypeSynapseQuery, PlatformSynapse},
		{pipeline.AssetTypeOracleQuery, PlatformOracle},
		{pipeline.AssetTypeFabricQuery, PlatformFabric},
		{pipeline.AssetTypeVerticaQuery, PlatformVertica},
		{pipeline.AssetTypePython, PlatformDefault},
	}

	for _, tt := range tests {
		t.Run(string(tt.assetType), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, PlatformForAssetType(tt.assetType))
		})
	}
}

func TestSlugify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"Hello World", "hello_world"},
		{"", ""},
		{"already_valid", "already_valid"},
		{"with-dashes", "with_dashes"},
		{"Special!@#Characters", "specialcharacters"},
		{"123starts_with_number", "_123starts_with_number"},
		{"MixedCase", "mixedcase"},
		{"multiple   spaces", "multiple_spaces"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, bruinSlugify(tt.input))
		})
	}
}
