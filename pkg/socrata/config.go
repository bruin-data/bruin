package socrata

import (
	"fmt"
	"net/url"
)

type Config struct {
	Domain    string
	DatasetID string
	AppToken  string
	Username  string
	Password  string
}

func (c *Config) GetIngestrURI() string {
	u := url.Values{}
	u.Add("domain", c.Domain)
	u.Add("dataset_id", c.DatasetID)
	u.Add("app_token", c.AppToken)

	if c.Username != "" {
		u.Add("username", c.Username)
	}

	if c.Password != "" {
		u.Add("password", c.Password)
	}

	return fmt.Sprintf("socrata://?%s", u.Encode())
}
