package bigquery

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBigQueryBuiltinOverrides(t *testing.T) {
	t.Parallel()

	renderer := jinja.NewRenderer(jinja.Context{
		"bruin": jinja.BuiltinFunctions(jinja.PlatformBigQuery),
	})

	t.Run("surrogate_key uses to_hex and string cast", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.generate_surrogate_key(['user_id', 'event_date']) }}")
		require.NoError(t, err)
		assert.Contains(t, result, "to_hex(md5(")
		assert.Contains(t, result, "cast(user_id as string)")
		assert.Contains(t, result, "cast(event_date as string)")
	})

	t.Run("deduplicate uses QUALIFY", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.deduplicate('my_table', 'user_id', 'updated_at desc') }}")
		require.NoError(t, err)
		assert.Contains(t, result, "qualify")
		assert.NotContains(t, result, "natural join")
	})

	t.Run("pivot quotes identifiers with backticks", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.pivot('status', ['active']) }}")
		require.NoError(t, err)
		assert.Contains(t, result, "as `active`")
		assert.NotContains(t, result, `as "active"`)
	})

	t.Run("haversine uses inline radians", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.haversine_distance('lat1', 'lon1', 'lat2', 'lon2') }}")
		require.NoError(t, err)
		assert.Contains(t, result, "acos(-1) / 180")
		assert.NotContains(t, result, "radians(")
	})

	t.Run("date_spine uses date_diff-backed array offsets", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render(`{{ bruin.date_spine('day', "'2020-01-01'", "'2025-01-01'") }}`)
		require.NoError(t, err)
		assert.Contains(t, result, "generate_array")
		assert.Contains(t, result, "date_diff")
		assert.Contains(t, result, "date_add")
		assert.NotContains(t, result, "generated_number <= 10000")
	})
}
