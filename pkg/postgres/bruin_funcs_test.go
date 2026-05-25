package postgres

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresBuiltinOverrides(t *testing.T) {
	t.Parallel()

	renderer := jinja.NewRenderer(jinja.Context{
		"bruin": jinja.BuiltinFunctions(jinja.PlatformPostgres),
	})

	t.Run("deduplicate uses DISTINCT ON", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.deduplicate('my_table', 'user_id', 'updated_at desc') }}")
		require.NoError(t, err)
		assert.Contains(t, result, "distinct on (user_id)")
		assert.Contains(t, result, "order by user_id, updated_at desc")
		assert.NotContains(t, result, "row_number")
	})

	t.Run("date_spine uses generate_series", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render(`{{ bruin.date_spine('day', "'2020-01-01'", "'2025-01-01'") }}`)
		require.NoError(t, err)
		assert.Contains(t, result, "generate_series")
		assert.Contains(t, result, "interval '1 day'")
	})
}
