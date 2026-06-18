package salesforce

import (
	"net/url"
	"strings"
	"testing"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    Config
		wantParam map[string]string
		notParam  []string
	}{
		{
			name: "access token takes precedence over username/password",
			config: Config{
				Username:    "user",
				Password:    "pass",
				Token:       "tok",
				AccessToken: "atk",
				Domain:      "company.my.salesforce.com",
			},
			wantParam: map[string]string{
				"access_token": "atk",
				"domain":       "company.my.salesforce.com",
			},
			notParam: []string{"username", "password", "token"},
		},
		{
			name: "username/password path when no access token",
			config: Config{
				Username: "user",
				Password: "pass",
				Token:    "tok",
				Domain:   "company.my.salesforce.com",
			},
			wantParam: map[string]string{
				"username": "user",
				"password": "pass",
				"token":    "tok",
				"domain":   "company.my.salesforce.com",
			},
			notParam: []string{"access_token"},
		},
		{
			name:   "empty config falls back to username path with empty values",
			config: Config{},
			wantParam: map[string]string{
				"username": "",
				"password": "",
				"token":    "",
				"domain":   "",
			},
			notParam: []string{"access_token"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uri := tt.config.GetIngestrURI()
			if !strings.HasPrefix(uri, "salesforce://?") {
				t.Fatalf("uri %q missing salesforce:// scheme", uri)
			}

			parsed, err := url.Parse(uri)
			if err != nil {
				t.Fatalf("failed to parse uri %q: %v", uri, err)
			}
			params := parsed.Query()

			for k, want := range tt.wantParam {
				if got := params.Get(k); got != want {
					t.Errorf("param %q = %q, want %q", k, got, want)
				}
			}
			for _, k := range tt.notParam {
				if params.Has(k) {
					t.Errorf("param %q should not be present, got %q", k, params.Get(k))
				}
			}
		})
	}
}
