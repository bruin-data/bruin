package cursor

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
		Scheme:   "cursor",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
