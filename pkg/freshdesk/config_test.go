package freshdesk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "basic config",
			config: Config{
				Domain: "mycompany",
				APIKey: "test-api-key-123",
			},
			expected: "freshdesk://mycompany?api_key=test-api-key-123",
		},
		{
			name: "with full domain",
			config: Config{
				Domain: "mycompany.freshdesk.com",
				APIKey: "secret-key",
			},
			expected: "freshdesk://mycompany.freshdesk.com?api_key=secret-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.config.GetIngestrURI()
			assert.Equal(t, tt.expected, result)
		})
	}
}
