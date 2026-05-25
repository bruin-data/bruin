package snowflake

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnowflakeBuiltinOverrides(t *testing.T) {
	t.Parallel()

	renderer := jinja.NewRenderer(jinja.Context{
		"bruin": jinja.BuiltinFunctions(jinja.PlatformSnowflake),
	})

	t.Run("deduplicate uses QUALIFY", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.deduplicate('my_table', 'user_id', 'updated_at desc') }}")
		require.NoError(t, err)
		assert.Contains(t, result, "qualify")
		assert.NotContains(t, result, "natural join")
	})

	t.Run("width_bucket uses native function", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.width_bucket('price', '0', '100', '10') }}")
		require.NoError(t, err)
		assert.Equal(t, "width_bucket(price, 0, 100, 10)", result)
	})
}
