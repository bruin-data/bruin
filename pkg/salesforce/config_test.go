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
		wantErr   string
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
			notParam: []string{"username", "password", "token", "client_id", "client_secret", "grant_type"},
		},
		{
			name: "access token takes precedence over client credentials",
			config: Config{
				AccessToken:  "atk",
				ClientID:     "cid",
				ClientSecret: "csecret",
				Domain:       "company.my.salesforce.com",
			},
			wantParam: map[string]string{
				"access_token": "atk",
				"domain":       "company.my.salesforce.com",
			},
			notParam: []string{"client_id", "client_secret", "grant_type", "username", "password", "token"},
		},
		{
			name: "client credentials path when no access token",
			config: Config{
				ClientID:     "cid",
				ClientSecret: "csecret",
				Domain:       "company.my.salesforce.com",
			},
			wantParam: map[string]string{
				"grant_type":    "client_credentials",
				"client_id":     "cid",
				"client_secret": "csecret",
				"domain":        "company.my.salesforce.com",
			},
			notParam: []string{"access_token", "username", "password", "token"},
		},
		{
			name: "client credentials take precedence over username/password",
			config: Config{
				Username:     "user",
				Password:     "pass",
				Token:        "tok",
				ClientID:     "cid",
				ClientSecret: "csecret",
				Domain:       "company.my.salesforce.com",
			},
			wantParam: map[string]string{
				"grant_type":    "client_credentials",
				"client_id":     "cid",
				"client_secret": "csecret",
				"domain":        "company.my.salesforce.com",
			},
			notParam: []string{"access_token", "username", "password", "token"},
		},
		{
			name: "username/password path when no access token or client credentials",
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
			notParam: []string{"access_token", "client_id", "client_secret", "grant_type"},
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
			notParam: []string{"access_token", "client_id", "client_secret", "grant_type"},
		},
		{
			name: "client_id without client_secret returns error",
			config: Config{
				ClientID: "cid",
				Domain:   "company.my.salesforce.com",
			},
			wantErr: "client_secret must be provided when client_id is set",
		},
		{
			name: "client_secret without client_id returns error",
			config: Config{
				ClientSecret: "csecret",
				Domain:       "company.my.salesforce.com",
			},
			wantErr: "client_id must be provided when client_secret is set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uri, err := tt.config.GetIngestrURI()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
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
