package salesforce

import (
	"net/url"
)

type Config struct {
	Username string
	Password string
	Token    string
}

func (c *Config) GetIngestrURI() string {
	//salesforce://?username=<your_username>&password=<your_password>&token=<your_token>
	baseURL := "salesforce://"
	params := url.Values{}
	params.Add("username", c.Username)
	params.Add("password", c.Password)
	params.Add("token", c.Token)
	return baseURL + "?" + params.Encode()
}
