package quickbooks

import "fmt"

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
	uri := fmt.Sprintf("quickbooks://?company_id=%s&client_id=%s&client_secret=%s&refresh_token=%s",
		c.CompanyID, c.ClientID, c.ClientSecret, c.RefreshToken)
	if c.Environment != "" {
		uri += "&environment=" + c.Environment
	}
	if c.MinorVersion != "" {
		uri += "&minor_version=" + c.MinorVersion
	}
	return uri
}
