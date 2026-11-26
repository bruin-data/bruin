package git

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestEnsureGivenPatternIsInGitignoreEndsWithNewline(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		existing string // "" means no .gitignore yet
		want     string
	}{
		"no file":                {want: ".bruin.yml\n"},
		"ends with newline":      {existing: "node_modules\n", want: "node_modules\n.bruin.yml\n"},
		"ends without a newline": {existing: "node_modules", want: "node_modules\n.bruin.yml\n"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := afero.NewMemMapFs()
			if tt.existing != "" {
				require.NoError(t, afero.WriteFile(fs, "repo/.gitignore", []byte(tt.existing), 0o644))
			}

			require.NoError(t, EnsureGivenPatternIsInGitignore(fs, "repo", ".bruin.yml"))

			got, err := afero.ReadFile(fs, "repo/.gitignore")
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}
