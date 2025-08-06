package wise

import "net/url"

// Config describes credentials for Wise connection.
type Config struct {
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

// GetIngestrURI builds the Wise ingestr URI.
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	return "wise://?" + params.Encode()
}
