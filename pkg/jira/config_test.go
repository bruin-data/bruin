package jira

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{
			name:   "without cloud_id",
			config: Config{Domain: "company.atlassian.net", Email: "user@company.com", APIToken: "tok"},
			want:   "jira://company.atlassian.net?api_token=tok&email=user%40company.com",
		},
		{
			name:   "with cloud_id",
			config: Config{Domain: "company.atlassian.net", Email: "user@company.com", APIToken: "tok", CloudID: "abc-123"},
			want:   "jira://company.atlassian.net?api_token=tok&cloud_id=abc-123&email=user%40company.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.config.GetIngestrURI())
		})
	}
}
