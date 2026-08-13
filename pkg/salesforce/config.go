package salesforce

import (
	"errors"
	"net/url"
)

type Config struct {
	Username     string
	Password     string
	Token        string
	AccessToken  string
	ClientID     string
	ClientSecret string
	Domain       string
}

func (c *Config) GetIngestrURI() (string, error) {
	params := url.Values{}
	switch {
	case c.AccessToken != "":
		params.Set("access_token", c.AccessToken)
	case c.ClientID != "" || c.ClientSecret != "":
		if c.ClientID == "" {
			return "", errors.New("salesforce: client_id must be provided when client_secret is set")
		}
		if c.ClientSecret == "" {
			return "", errors.New("salesforce: client_secret must be provided when client_id is set")
		}
		params.Set("grant_type", "client_credentials")
		params.Set("client_id", c.ClientID)
		params.Set("client_secret", c.ClientSecret)
	default:
		params.Set("username", c.Username)
		params.Set("password", c.Password)
		params.Set("token", c.Token)
	}
	params.Set("domain", c.Domain)
	return "salesforce://?" + params.Encode(), nil
}
