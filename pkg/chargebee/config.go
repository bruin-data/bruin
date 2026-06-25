package chargebee

import (
	"fmt"
)

type Config struct {
	Site   string
	APIKey string
}

func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("chargebee://%s?api_key=%s", c.Site, c.APIKey)
}
