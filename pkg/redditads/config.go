package redditads

import (
	"errors"
	"net/url"
)

type Config struct {
	AccessToken  string `yaml:"access_token" json:"access_token" mapstructure:"access_token"`
	ClientID     string `yaml:"client_id" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret" json:"client_secret" mapstructure:"client_secret"`
	RefreshToken string `yaml:"refresh_token" json:"refresh_token" mapstructure:"refresh_token"`
}

func (c *Config) GetIngestrURI() (string, error) {
	// Auth: either a ready access_token, or the OAuth app credentials + refresh
	// token to mint one (access tokens expire ~24h; the refresh token is permanent).
	// The three OAuth fields are all-or-nothing — partial sets are never forwarded.
	hasRefreshCreds := c.ClientID != "" && c.ClientSecret != "" && c.RefreshToken != ""
	if c.AccessToken == "" && !hasRefreshCreds {
		return "", errors.New("reddit_ads: either access_token, or client_id + client_secret + refresh_token, must be provided")
	}

	params := url.Values{}
	if c.AccessToken != "" {
		params.Set("access_token", c.AccessToken)
	}
	if hasRefreshCreds {
		params.Set("client_id", c.ClientID)
		params.Set("client_secret", c.ClientSecret)
		params.Set("refresh_token", c.RefreshToken)
	}

	return "redditads://?" + params.Encode(), nil
}
