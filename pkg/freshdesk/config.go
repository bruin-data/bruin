package freshdesk

import (
	"net/url"
)

type Config struct {
	Domain string
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	u := &url.URL{
		Scheme:   "freshdesk",
		Host:     c.Domain,
		RawQuery: url.Values{"api_key": {c.APIKey}}.Encode(),
	}
	return u.String()
}
