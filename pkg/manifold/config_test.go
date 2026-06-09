package manifold

import "testing"

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{
			name:   "empty config",
			config: Config{},
			want:   "manifold://",
		},
		{
			name: "query parameters",
			config: Config{
				QueryParams: map[string]string{
					"term": "bitcoin",
					"sort": "newest",
				},
			},
			want: "manifold://?sort=newest&term=bitcoin",
		},
		{
			name: "encodes special characters",
			config: Config{
				QueryParams: map[string]string{
					"contract_slug": "will bitcoin?",
					"market_id":     "abc/123",
				},
			},
			want: "manifold://?contract_slug=will+bitcoin%3F&market_id=abc%2F123",
		},
		{
			name: "repeated parameters",
			config: Config{
				QueryParamLists: map[string][]string{
					"ids": {"market-1", "market-2"},
				},
			},
			want: "manifold://?ids=market-1&ids=market-2",
		},
		{
			name: "skips empty keys and values",
			config: Config{
				QueryParams: map[string]string{
					"":       "ignored",
					"term":   "",
					"userId": "user-1",
				},
				QueryParamLists: map[string][]string{
					"":    {"ignored"},
					"ids": {"", "market-1"},
				},
			},
			want: "manifold://?ids=market-1&userId=user-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.config.GetIngestrURI(); got != tt.want {
				t.Errorf("GetIngestrURI() = %v, want %v", got, tt.want)
			}
		})
	}
}
