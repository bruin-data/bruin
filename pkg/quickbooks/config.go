package quickbooks

import (
	"net/url"
)

// Config holds credentials for QuickBooks connection.
type Config struct {
	CompanyID    string `yaml:"company_id" json:"company_id" mapstructure:"company_id"`
	ClientID     string `yaml:"client_id" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret" json:"client_secret" mapstructure:"client_secret"`
	RefreshToken string `yaml:"refresh_token" json:"refresh_token" mapstructure:"refresh_token"`
	Environment  string `yaml:"environment,omitempty" json:"environment,omitempty" mapstructure:"environment"`
	MinorVersion string `yaml:"minor_version,omitempty" json:"minor_version,omitempty" mapstructure:"minor_version"`
}

// GetIngestrURI builds an ingestr URI for QuickBooks.
func (c *Config) GetIngestrURI() string {
	u := url.Values{}
	u.Set("company_id", c.CompanyID)
	u.Set("client_id", c.ClientID)
	u.Set("client_secret", c.ClientSecret)
	u.Set("refresh_token", c.RefreshToken)

	if c.Environment != "" {
		u.Set("environment", c.Environment)
	}
	if c.MinorVersion != "" {
		u.Set("minor_version", c.MinorVersion)
	}
	return "quickbooks://?" + u.Encode()
}
