package snapchatads

import "net/url"

type Config struct {
	RefreshToken   string
	ClientID       string
	ClientSecret   string
	OrganizationID string
}

func (c *Config) GetIngestrURI() string {
	uri := "snapchatads://?refresh_token=" + url.QueryEscape(c.RefreshToken) +
		"&client_id=" + url.QueryEscape(c.ClientID) +
		"&client_secret=" + url.QueryEscape(c.ClientSecret)

	if c.OrganizationID != "" {
		uri += "&organization_id=" + url.QueryEscape(c.OrganizationID)
	}

	return uri
}
