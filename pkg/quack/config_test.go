package quack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	disabled := true
	tests := []struct {
		name    string
		config  Config
		want    string
		wantErr string
	}{
		{
			name:   "host only",
			config: Config{Host: "localhost"},
			want:   "quack://localhost",
		},
		{
			name:   "all fields",
			config: Config{Host: "db.internal", Port: 9494, Token: "s3cr&t=", DisableSSL: &disabled},
			want:   "quack://db.internal:9494?disable_ssl=true&token=s3cr%26t%3D",
		},
		{
			name:   "ipv6 without port",
			config: Config{Host: "::1"},
			want:   "quack://[::1]",
		},
		{
			name:   "ipv6 with port",
			config: Config{Host: "::1", Port: 9494},
			want:   "quack://[::1]:9494",
		},
		{
			name:    "missing host",
			config:  Config{Host: "  "},
			wantErr: "quack: host must be provided",
		},
		{
			name:    "invalid port",
			config:  Config{Host: "localhost", Port: 70000},
			wantErr: "quack: port must be between 1 and 65535, got 70000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.config.GetIngestrURI()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
