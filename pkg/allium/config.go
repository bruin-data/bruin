package allium

import "net/url"

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	if c.APIKey != "" {
		params.Set("api_key", c.APIKey)
	}

	uri := "allium://"
	if len(params) > 0 {
		uri += "?" + params.Encode()
	}

	return uri
}
