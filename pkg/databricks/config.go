package databricks

import (
	"fmt"
	"net/url"
)

type Config struct {
	Token string
	Host  string
	// Path will determine cluster or sql warehouse. See https://docs.databricks.com/en/dev-tools/go-sql-driver.html#connect-with-a-dsn-connection-string
	Path string
}

func (c *Config) ToDBConnectionURI() string {
	u := &url.URL{
		User: url.UserPassword("token", c.Token),
		Host: fmt.Sprintf("%s:%d", c.Host),
		Path: c.Path,
	}

	return u.String()
}
