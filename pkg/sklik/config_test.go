package sklik

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptr(v int64) *int64 { return &v }

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		want    string
		wantErr bool
	}{
		{
			name:   "token only",
			config: Config{Token: "tok-123"},
			want:   "sklik://?token=tok-123",
		},
		{
			name:   "token with user_id",
			config: Config{Token: "tok-123", UserID: ptr(456)},
			want:   "sklik://?token=tok-123&user_id=456",
		},
		{
			name:   "token is trimmed",
			config: Config{Token: "  tok-123  ", UserID: ptr(456)},
			want:   "sklik://?token=tok-123&user_id=456",
		},
		{
			name:   "special characters are escaped",
			config: Config{Token: "tok/with+special=chars"},
			want:   "sklik://?token=tok%2Fwith%2Bspecial%3Dchars",
		},
		{
			name:    "missing token",
			config:  Config{},
			wantErr: true,
		},
		{
			name:    "blank token",
			config:  Config{Token: "   "},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.config.GetIngestrURI()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewClient(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{Token: "tok-123"})
	require.NoError(t, err)
	require.NotNil(t, client)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "sklik://?token=tok-123", uri)
}
