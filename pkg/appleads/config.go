package appleads

import (
	"net/url"
)

type Config struct {
	ClientID  string
	TeamID    string
	KeyID     string
	OrgID     string
	KeyPath   string
	KeyBase64 string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("client_id", c.ClientID)
	params.Set("team_id", c.TeamID)
	params.Set("key_id", c.KeyID)
	params.Set("org_id", c.OrgID)

	switch {
	case c.KeyPath != "":
		params.Set("key_path", c.KeyPath)
	case c.KeyBase64 != "":
		params.Set("key_base64", c.KeyBase64)
	}

	uri := url.URL{
		Scheme:   "appleads",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
