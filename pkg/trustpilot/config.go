package trustpilot

import "net/url"

// Config holds credentials for Trustpilot connection.
type Config struct {
	BusinessUnitID string `yaml:"business_unit_id" json:"business_unit_id" mapstructure:"business_unit_id"`
	APIKey         string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

// GetIngestrURI builds the ingestr URI for Trustpilot.
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("api_key", c.APIKey)
	return "trustpilot://" + c.BusinessUnitID + "?" + params.Encode()
}
