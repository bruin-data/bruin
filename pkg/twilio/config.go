package twilio

import "net/url"

type Config struct {
	AccountSID string
	AuthToken  string
	APIKey     string
	APISecret  string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("account_sid", c.AccountSID)
	if c.AuthToken != "" {
		params.Set("auth_token", c.AuthToken)
	}
	if c.APIKey != "" {
		params.Set("api_key", c.APIKey)
	}
	if c.APISecret != "" {
		params.Set("api_secret", c.APISecret)
	}
	return "twilio://?" + params.Encode()
}
