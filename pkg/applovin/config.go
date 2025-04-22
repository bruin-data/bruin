package applovin

import (
	"net/url"
)

type Config struct {
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	// applovin://?api_key=<your_api_key>
	baseURL := "applovin://"
	params := url.Values{}
	params.Add("api_key", c.APIKey)
	return baseURL + "?" + params.Encode()
}
