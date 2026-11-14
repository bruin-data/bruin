package db2

import (
	"fmt"
	"net/url"
	"strconv"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     string
	Database string
	Schema   string
	SSL      bool
	Timeout  int
	Platform string
}

func (c *Config) GetIngestrURI() string {
	// db2://<username>:<password>@<host>:<port>/<database-name>
	u := &url.URL{
		Scheme: "db2",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%s", c.Host, c.Port),
		Path:   "/" + c.Database,
	}

	q := u.Query()
	if c.Schema != "" {
		q.Set("schema", c.Schema)
	}
	if c.SSL {
		q.Set("ssl", "true")
	}
	if c.Timeout > 0 {
		q.Set("timeout", strconv.Itoa(c.Timeout))
	}
	if c.Platform != "" {
		q.Set("platform", c.Platform)
	}
	u.RawQuery = q.Encode()

	return u.String()
}
