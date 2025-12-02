package primer

import "net/url"

// Config describes credentials for Primer connection.
type Config struct {
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

// GetIngestrURI builds the Primer ingestr URI.
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	return "primer://?" + params.Encode()
}
