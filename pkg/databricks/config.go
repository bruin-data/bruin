package databricks

import (
	"fmt"
	"strings"
)

type Config struct {
	Token string
	Host  string
	Port  int
	// Path will determine cluster or sql warehouse. See https://docs.databricks.com/en/dev-tools/go-sql-driver.html#connect-with-a-dsn-connection-string
	Path string
}

func (c *Config) ToDBConnectionURI() string {
	return fmt.Sprintf("token:%s@%s:%d/%s", c.Token, c.Host, c.Port, strings.TrimPrefix(c.Path, "/"))
}
