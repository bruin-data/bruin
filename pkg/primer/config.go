package primer

import "net/url"

// Config describes credentials for Primer connection.
type Config struct {
	APIKey     string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
	APIVersion string `yaml:"api_version,omitempty" json:"api_version,omitempty" mapstructure:"api_version"`
}

// GetIngestrURI builds the Primer ingestr URI.
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if c.APIVersion != "" {
		params.Set("api_version", c.APIVersion)
	}
	return "primer://?" + params.Encode()
}
