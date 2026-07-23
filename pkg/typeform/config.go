package typeform

import "net/url"

type Config struct {
	Token  string
	Region string
}

func (c *Config) GetIngestrURI() string {
	uri := "typeform://?token=" + url.QueryEscape(c.Token)
	if c.Region != "" {
		uri += "&region=" + url.QueryEscape(c.Region)
	}
	return uri
}
