package vertica

import (
	"fmt"
	"net/url"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Schema   string
}

func (c *Config) ToDBConnectionURI() string {
	query := url.Values{}
	if c.Schema != "" {
		query.Add("search_path", c.Schema)
	}

	u := &url.URL{
		Scheme:   "vertica",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     c.Database,
		RawQuery: query.Encode(),
	}

	return u.String()
}

func (c *Config) GetIngestrURI() string {
	u := &url.URL{
		Scheme: "vertica",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.Database,
	}

	return u.String()
}
