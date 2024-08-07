package databricks

import (
	"fmt"
	"strings"
)

type Config struct {
	Token string
	Host  string
	// Path will determine cluster or sql warehouse. See https://docs.databricks.com/en/dev-tools/go-sql-driver.html#connect-with-a-dsn-connection-string
	Path string
}

func (c *Config) ToDBConnectionURI() string {
	return fmt.Sprintf("token:%s@%s/%s", c.Token, c.Host, strings.TrimPrefix(c.Path, "/"))
}
