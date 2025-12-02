package bruincloud

import (
	"net/url"
)

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_token", c.APIKey)

	uri := url.URL{
		Scheme:   "bruin",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
