package granola

import "net/url"

// Config holds credentials for a Granola connection.
type Config struct {
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

// GetIngestrURI builds the Granola ingestr URI.
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	return "granola://?" + params.Encode()
}
