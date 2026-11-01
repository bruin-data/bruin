package enhance

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestDefaultModels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provider Provider
		want     string
	}{
		{"claude", NewClaudeProvider("", afero.NewMemMapFs()), "claude-sonnet-5-5"},
		{"opencode", NewOpenCodeProvider("", afero.NewMemMapFs()), "anthropic/claude-sonnet-5-5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.provider.(*CLIProvider).model)
		})
	}
}
