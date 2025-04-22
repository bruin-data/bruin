package applovin

import (
	"fmt"
	"net/url"
)

type Config struct {
	APIKey string
}

// applovin://?api_key=<your_api_key>
func (c *Config) GetIngestrURI() string {
	// applovin://?api_key=<your_api_key>
	baseURL := "applovin://"
	params := url.Values{}
	params.Add("api_key", c.APIKey)
	fmt.Println(baseURL + "?" + params.Encode())
	return baseURL + "?" + params.Encode()
}
