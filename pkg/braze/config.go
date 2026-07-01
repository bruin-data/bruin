package braze

import "net/url"

type Config struct {
	APIKey   string
	Endpoint string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	params.Set("endpoint", c.Endpoint)
	return "braze://?" + params.Encode()
}
