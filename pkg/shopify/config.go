package shopify

import (
	"fmt"
)

type Config struct {
	URL          string
	APIKey       string
	ClientID     string
	ClientSecret string
}

func (c *Config) GetIngestrURI() string {
	uri := fmt.Sprintf("shopify://%s?api_key=%s", c.URL, c.APIKey)
	if c.ClientID != "" {
		uri += fmt.Sprintf("&client_id=%s", c.ClientID)
	}
	if c.ClientSecret != "" {
		uri += fmt.Sprintf("&client_secret=%s", c.ClientSecret)
	}
	return uri
}
