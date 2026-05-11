package smartsheet

import (
	"net/url"
)

type Config struct {
	AccessToken  string
	SmartsheetID string
}

func (c *Config) GetIngestrURI() string {
	q := url.Values{}
	q.Set("access_token", c.AccessToken)
	if c.SmartsheetID != "" {
		q.Set("smartsheet_id", c.SmartsheetID)
	}
	return "smartsheet://?" + q.Encode()
}
