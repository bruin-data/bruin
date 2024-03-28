package mongo

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
}

func (c *Config) GetIngestrURI() string {
	u := &url.URL{
		Scheme: "mongodb",
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.Database,
	}

	if c.Username != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}

	return u.String()
}
