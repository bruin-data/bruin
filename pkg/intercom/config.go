package intercom

import (
	"net/url"
)

type Config struct {
	AccessToken string
	Region      string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("access_token", c.AccessToken)
	if c.Region != "" {
		params.Set("region", c.Region)
	}

	uri := url.URL{
		Scheme:   "intercom",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
