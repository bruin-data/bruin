package mssql

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMSSQLBuiltinOverrides(t *testing.T) {
	t.Parallel()

	renderer := jinja.NewRenderer(jinja.Context{
		"bruin": jinja.BuiltinFunctions(jinja.PlatformMSSQL),
	})

	t.Run("surrogate_key uses hashbytes and casts to unbounded varchar", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.generate_surrogate_key(['user_id', 'session_id']) }}")
		require.NoError(t, err)
		assert.Contains(t, result, "hashbytes('md5',")
		assert.Contains(t, result, "convert(varchar(32),")
		assert.Contains(t, result, "cast(user_id as varchar(max))")
		assert.Contains(t, result, "cast(session_id as varchar(max))")
		assert.NotContains(t, result, "cast(user_id as varchar)")
	})

	t.Run("deduplicate uses TOP WITH TIES", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.deduplicate('my_table', 'user_id', 'updated_at desc') }}")
		require.NoError(t, err)
		assert.Contains(t, result, "select top (1) with ties *")
		assert.Contains(t, result, "order by row_number() over")
		assert.NotContains(t, result, "_bruin_dedup_rn")
		assert.NotContains(t, result, "natural join")
		assert.NotContains(t, result, "qualify")
	})
}
