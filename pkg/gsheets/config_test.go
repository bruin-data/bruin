package gsheets

import (
	"encoding/base64"
	"testing"
)

func TestGetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{
			name:   "no credentials falls back to Application Default Credentials",
			config: Config{},
			want:   "gsheets://",
		},
		{
			name:   "service account json is base64 encoded",
			config: Config{ServiceAccountJSON: `{"type":"service_account"}`},
			want:   "gsheets://?credentials_base64=" + base64.StdEncoding.EncodeToString([]byte(`{"type":"service_account"}`)),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.config.GetIngestrURI(); got != tt.want {
				t.Errorf("GetIngestrURI() = %q, want %q", got, tt.want)
			}
		})
	}
}
