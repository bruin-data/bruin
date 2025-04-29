package db2

import (
	"fmt"
	"net/url"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     string
	Database string
}

func (c *Config) GetIngestrURI() string {
	// db2://<username>:<password>@<host>:<port>/<database-name>
	u := &url.URL{
		Scheme: "db2",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%s", c.Host, c.Port),
		Path:   "/" + c.Database,
	}
	return u.String()
}
