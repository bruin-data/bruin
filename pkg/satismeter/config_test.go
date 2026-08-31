package satismeter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		want    string
		wantErr bool
	}{
		{
			name:   "valid credentials",
			config: Config{APIKey: "sm_abc123", ProjectID: "5bb480aaebf3ed0004c6f3dd"},
			want:   "satismeter://?api_key=sm_abc123&project_id=5bb480aaebf3ed0004c6f3dd",
		},
		{
			name:   "values are trimmed",
			config: Config{APIKey: "  sm_abc123 ", ProjectID: " proj-1 "},
			want:   "satismeter://?api_key=sm_abc123&project_id=proj-1",
		},
		{
			name:    "missing api key",
			config:  Config{ProjectID: "proj-1"},
			wantErr: true,
		},
		{
			name:    "missing project id",
			config:  Config{APIKey: "sm_abc123"},
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
