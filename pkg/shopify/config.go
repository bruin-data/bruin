package shopify

import (
	"fmt"
)

type Config struct {
	URL    string
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("gorgias://%s?api_key=%s", c.URL, c.APIKey)
}
