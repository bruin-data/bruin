package quickbooks

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
			name:   "refresh token",
			config: Config{CompanyID: "123", ClientID: "cid", ClientSecret: "sec", RefreshToken: "rtok"},
			want:   "quickbooks://?client_id=cid&client_secret=sec&company_id=123&refresh_token=rtok",
		},
		{
			name:   "access token only",
			config: Config{CompanyID: "123", ClientID: "cid", ClientSecret: "sec", AccessToken: "a&tok", Environment: "sandbox", MinorVersion: "75"},
			want:   "quickbooks://?access_token=a%26tok&client_id=cid&client_secret=sec&company_id=123&environment=sandbox&minor_version=75",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.config.GetIngestrURI())
		})
	}
}
