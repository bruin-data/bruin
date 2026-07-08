package synapse

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSynapseBuiltinOverrides(t *testing.T) {
	t.Parallel()

	renderer := jinja.NewRenderer(jinja.Context{
		"bruin": jinja.BuiltinFunctions(jinja.PlatformSynapse),
	})

	t.Run("surrogate_key casts to unbounded varchar", func(t *testing.T) {
		t.Parallel()
		result, err := renderer.Render("{{ bruin.generate_surrogate_key(['user_id', 'session_id']) }}")
		require.NoError(t, err)
		assert.Contains(t, result, "cast(user_id as varchar(max))")
		assert.Contains(t, result, "cast(session_id as varchar(max))")
		assert.NotRegexp(t, `cast\(user_id as varchar\s*\)`, result)
		assert.Contains(t, result, "hashbytes('md5',")
	})
}
