package customerio

import "net/url"

type Config struct {
	APIKey string
	Region string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Add("api_key", c.APIKey)
	if c.Region != "" {
		params.Add("region", c.Region)
	}
	return "customerio://?" + params.Encode()
}
