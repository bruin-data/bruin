package zoom

import "net/url"

// Config holds credentials for Zoom connection.
type Config struct {
	ClientID     string `yaml:"client_id" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret" json:"client_secret" mapstructure:"client_secret"`
	AccountID    string `yaml:"account_id" json:"account_id" mapstructure:"account_id"`
}

// GetIngestrURI builds an ingestr URI for Zoom.
func (c *Config) GetIngestrURI() string {
	u := url.Values{}
	u.Set("client_id", c.ClientID)
	u.Set("client_secret", c.ClientSecret)
	u.Set("account_id", c.AccountID)
	return "zoom://?" + u.Encode()
}
