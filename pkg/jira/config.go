package jira

import (
	"net/url"
)

type Config struct {
	Domain   string
	Email    string
	APIToken string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("email", c.Email)
	params.Set("api_token", c.APIToken)
	uri := url.URL{
		Scheme:   "jira",
		Host:     c.Domain,
		RawQuery: params.Encode(),
	}
	return uri.String()
}
