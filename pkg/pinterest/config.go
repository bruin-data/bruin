package pinterest

import "net/url"

// Config holds credentials for Pinterest connection.
type Config struct {
	AccessToken string `yaml:"access_token" json:"access_token" mapstructure:"access_token"`
}

// GetIngestrURI builds an ingestr URI for Pinterest.
func (c *Config) GetIngestrURI() string {
	u := url.Values{}
	u.Set("access_token", c.AccessToken)
	return "pinterest://?" + u.Encode()
}
