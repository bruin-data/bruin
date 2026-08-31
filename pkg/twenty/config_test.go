package twenty

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(v int) *int           { return &v }
func floatPtr(v float64) *float64 { return &v }
func boolPtr(v bool) *bool        { return &v }

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		want    string
		wantErr bool
	}{
		{
			name:   "host and api key only",
			config: Config{Host: "api.twenty.com", APIKey: "eyJ123"},
			want:   "twenty://api.twenty.com?api_key=eyJ123",
		},
		{
			name: "all optional parameters",
			config: Config{
				Host:           "crm.example.com",
				APIKey:         "eyJ123",
				Scheme:         "http",
				BasePath:       "/rest",
				PageSize:       intPtr(150),
				RateLimit:      floatPtr(2.5),
				IncludeDeleted: boolPtr(false),
			},
			want: "twenty://crm.example.com?api_key=eyJ123&base_path=%2Frest&include_deleted=false&page_size=150&rate_limit=2.5&scheme=http",
		},
		{
			name:   "include_deleted true is emitted",
			config: Config{Host: "api.twenty.com", APIKey: "eyJ123", IncludeDeleted: boolPtr(true)},
			want:   "twenty://api.twenty.com?api_key=eyJ123&include_deleted=true",
		},
		{
			name:   "values are trimmed",
			config: Config{Host: " api.twenty.com ", APIKey: " eyJ123 "},
			want:   "twenty://api.twenty.com?api_key=eyJ123",
		},
		{
			name:    "missing host",
			config:  Config{APIKey: "eyJ123"},
			wantErr: true,
		},
		{
			name:    "missing api key",
			config:  Config{Host: "api.twenty.com"},
			wantErr: true,
		},
		{
			name:    "non-positive page size",
			config:  Config{Host: "api.twenty.com", APIKey: "eyJ123", PageSize: intPtr(0)},
			wantErr: true,
		},
		{
			name:    "non-positive rate limit",
			config:  Config{Host: "api.twenty.com", APIKey: "eyJ123", RateLimit: floatPtr(0)},
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

	client, err := NewClient(Config{Host: "api.twenty.com", APIKey: "eyJ123"})
	require.NoError(t, err)
	require.NotNil(t, client)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "twenty://api.twenty.com?api_key=eyJ123", uri)
}
