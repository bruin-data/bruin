package indeed

import (
	"net/url"
)

type Config struct {
	ClientID     string
	ClientSecret string
	EmployerID   string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Add("client_id", c.ClientID)
	params.Add("client_secret", c.ClientSecret)
	params.Add("employer_id", c.EmployerID)
	return "indeed://?" + params.Encode()
}
