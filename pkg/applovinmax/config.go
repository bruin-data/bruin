package applovinmax

import "fmt"

type Config struct {
	APIKey      string
	Application string
}

// applovinmax://?api_key=<your_api_key>&application=<application_name>
func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("applovinmax://?api_key=%s&application=%s", c.APIKey, c.Application)
}
