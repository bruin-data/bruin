package applovinmax

import "fmt"

type Config struct {
	APIKey string
}

// applovinmax://?api_key=<your_api_key>
func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("applovinmax://?api_key=%s", c.APIKey)
}
