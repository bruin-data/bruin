package recurly

import (
	"net/url"
)

type Config struct {
	APIKey string
	Region string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if c.Region != "" {
		params.Set("region", c.Region)
	}
	return "recurly://?" + params.Encode()
}
