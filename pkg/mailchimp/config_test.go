package mailchimp

import "testing"

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{
			name: "basic configuration",
			config: Config{
				APIKey: "test-api-key-123",
				Server: "us10",
			},
			want: "mailchimp://?api_key=test-api-key-123&server=us10",
		},
		{
			name: "different server region",
			config: Config{
				APIKey: "another-key",
				Server: "us19",
			},
			want: "mailchimp://?api_key=another-key&server=us19",
		},
		{
			name: "special characters in api key",
			config: Config{
				APIKey: "key-with-special&chars=test",
				Server: "eu1",
			},
			want: "mailchimp://?api_key=key-with-special%26chars%3Dtest&server=eu1",
		},
		{
			name: "empty api key",
			config: Config{
				APIKey: "",
				Server: "us10",
			},
			want: "mailchimp://?server=us10",
		},
		{
			name: "empty server",
			config: Config{
				APIKey: "test-key",
				Server: "",
			},
			want: "mailchimp://?api_key=test-key",
		},
		{
			name:   "empty config",
			config: Config{},
			want:   "mailchimp://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.config.GetIngestrURI()
			if got != tt.want {
				t.Errorf("GetIngestrURI() = %v, want %v", got, tt.want)
			}
		})
	}
}
