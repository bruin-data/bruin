package sftp

import (
	"fmt"
	"net/url"
)

type Config struct {
	Host     string
	Port     int
	Username string
	Password string
}

func (c *Config) GetIngestrURI() string {
	uri := url.URL{
		Scheme: "sftp",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
	}
	return uri.String()
}
