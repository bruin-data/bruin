package jinja

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:paralleltest // t.Setenv forbids parallel subtests
func TestRendererFilesystemRestrictionCannotBeDisabled(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "secret.sql")
	require.NoError(t, os.WriteFile(secret, []byte("outside-sentinel"), 0o600))
	for _, value := range []string{"", "true", "false"} {
		t.Run(fmt.Sprintf("flag=%q", value), func(t *testing.T) {
			t.Setenv(RestrictTemplateFilesystemEnv, value)
			for _, directive := range []string{
				`{%% include %q %%}`, `{%% import %q as m %%}`, `{%% from %q import thing %%}`, `{%% extends %q %%}`,
			} {
				out, err := NewRenderer(Context{}).Render(fmt.Sprintf(directive, filepath.ToSlash(secret)))
				require.Error(t, err)
				require.Empty(t, out)
				require.NotContains(t, err.Error(), "outside-sentinel")
			}
			out, err := NewRenderer(Context{}).Render(`{% for i in [1, 2, 3] %}{{ i }}{% endfor %}`)
			require.NoError(t, err)
			require.Equal(t, "123", out)
		})
	}
}
