package hostaway

import (
	"net/url"
)

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)

	uri := url.URL{
		Scheme:   "hostaway",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
