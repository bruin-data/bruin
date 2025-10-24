package socrata

import "net/url"

type Config struct {
	Domain    string
	DatasetID string
	AppToken  string
	Username  string
	Password  string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	if c.Domain != "" {
		params.Set("domain", c.Domain)
	}
	if c.DatasetID != "" {
		params.Set("dataset_id", c.DatasetID)
	}
	if c.AppToken != "" {
		params.Set("app_token", c.AppToken)
	}
	if c.Username != "" {
		params.Set("username", c.Username)
	}
	if c.Password != "" {
		params.Set("password", c.Password)
	}

	uri := "socrata://"
	if len(params) > 0 {
		uri += "?" + params.Encode()
	}

	return uri
}
