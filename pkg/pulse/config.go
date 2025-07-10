package pulse

import (
	"errors"
	"net/url"
)

// Config describes credentials for Pulse connection.
type Config struct {
	Token string `yaml:"token" json:"token" mapstructure:"token"`
}

// GetIngestrURI builds the Pulse ingestr URI.
func (c Config) GetIngestrURI() (string, error) {
	token := c.Token
	if token == "" {
		return "", errors.New("token is required")
	}
	u := url.URL{
		Scheme: "isoc-pulse",
		RawQuery: url.Values{
			"token": []string{token},
		}.Encode(),
	}
	return u.String(), nil
}
