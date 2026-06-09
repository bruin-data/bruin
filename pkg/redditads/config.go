package redditads

import (
	"errors"
	"net/url"
)

type Config struct {
	AccessToken string `yaml:"access_token" json:"access_token" mapstructure:"access_token"`
	AccountIds  string `yaml:"account_ids" json:"account_ids" mapstructure:"account_ids"`
}

func (c *Config) GetIngestrURI() (string, error) {
	if c.AccessToken == "" {
		return "", errors.New("reddit_ads: access_token must be provided")
	}

	params := url.Values{}
	params.Set("access_token", c.AccessToken)
	if c.AccountIds != "" {
		params.Set("account_ids", c.AccountIds)
	}

	return "redditads://?" + params.Encode(), nil
}
