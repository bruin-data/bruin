package shopify

import (
	"fmt"
	"net/url"
)

type Config struct {
	URL          string
	APIKey       string
	ClientID     string
	ClientSecret string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if c.ClientID != "" {
		params.Set("client_id", c.ClientID)
	}
	if c.ClientSecret != "" {
		params.Set("client_secret", c.ClientSecret)
	}
	return fmt.Sprintf("shopify://%s?%s", c.URL, params.Encode())
}
