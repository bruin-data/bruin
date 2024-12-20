package asana

import (
	"net/url"
)

type Config struct {
	WorkspaceID string
	AccessToken string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("access_token", c.AccessToken)
	uri := url.URL{
		Scheme:   "asana",
		Host:     c.WorkspaceID,
		RawQuery: params.Encode(),
	}
	return uri.String()
}
