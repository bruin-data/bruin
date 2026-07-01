package salesforce

import (
	"net/url"
)

type Config struct {
	Username    string
	Password    string
	Token       string
	AccessToken string
	Domain      string
}

func (c *Config) GetIngestrURI() string {
	baseURL := "salesforce://"
	params := url.Values{}
	if c.AccessToken != "" {
		params.Add("access_token", c.AccessToken)
	} else {
		params.Add("username", c.Username)
		params.Add("password", c.Password)
		params.Add("token", c.Token)
	}
	params.Add("domain", c.Domain)
	return baseURL + "?" + params.Encode()
}
