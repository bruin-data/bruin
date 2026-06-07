package redditads

import "net/url"

type Config struct {
	AccessToken string `yaml:"access_token" json:"access_token" mapstructure:"access_token"`
	AccountIds  string `yaml:"account_ids" json:"account_ids" mapstructure:"account_ids"`
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("access_token", c.AccessToken)
	params.Set("account_ids", c.AccountIds)
	return "redditads://?" + params.Encode()
}
