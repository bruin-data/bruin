package salesforce

import (
	"fmt"
	"net/url"
	"strings"
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
	clientID := strings.TrimSpace(c.ClientID)
	clientSecret := strings.TrimSpace(c.ClientSecret)

	if c.AccessToken != "" {
		params.Set("access_token", c.AccessToken)
	} else if clientID != "" || clientSecret != "" {
		if clientID == "" {
			return "", fmt.Errorf("salesforce: client_id must be provided when client_secret is set")
		}
		if clientSecret == "" {
			return "", fmt.Errorf("salesforce: client_secret must be provided when client_id is set")
		}
		params.Set("grant_type", "client_credentials")
		params.Set("client_id", clientID)
		params.Set("client_secret", clientSecret)
	} else {
		params.Set("username", c.Username)
		params.Set("password", c.Password)
		params.Set("token", c.Token)
	}
	params.Set("domain", c.Domain)
	return "salesforce://?" + params.Encode(), nil
}
