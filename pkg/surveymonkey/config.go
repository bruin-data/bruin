package surveymonkey

import "net/url"

type Config struct {
	AccessToken string
	Region      string
}

func (c *Config) GetIngestrURI() string {
	uri := "surveymonkey://?access_token=" + url.QueryEscape(c.AccessToken)
	if c.Region != "" {
		uri += "&region=" + url.QueryEscape(c.Region)
	}
	return uri
}
