package surveymonkey

import "net/url"

type Config struct {
	AccessToken string
	Datacenter  string
}

func (c *Config) GetIngestrURI() string {
	uri := "surveymonkey://?access_token=" + url.QueryEscape(c.AccessToken)
	if c.Datacenter != "" {
		uri += "&datacenter=" + url.QueryEscape(c.Datacenter)
	}
	return uri
}
