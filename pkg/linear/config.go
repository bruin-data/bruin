package linear

import "net/url"

// Config holds credentials for Linear connection.
type Config struct {
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

// GetIngestrURI builds the Linear ingestr URI.
func (c *Config) GetIngestrURI() string {
	u := url.Values{}
	u.Set("api_key", c.APIKey)
	return "linear://?" + u.Encode()
}
