package wise

import "net/url"

// Config describes credentials for Wise connection.
type Config struct {
	APIToken string `yaml:"api_token" json:"api_token" mapstructure:"api_token"`
}

// GetIngestrURI builds the Wise ingestr URI.
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_token", c.APIToken)
	return "wise://?" + params.Encode()
}
