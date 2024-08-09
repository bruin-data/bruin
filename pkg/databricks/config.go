package databricks

import (
	"fmt"
	"net/url"
	"strings"
)

type Config struct {
	// Token is the Databricks Personal API token
	Token string
	Host  string
	Port  int
	// Path will determine cluster or sql warehouse. See https://docs.databricks.com/en/dev-tools/go-sql-driver.html#connect-with-a-dsn-connection-string
	Path    string
	Catalog string
	Schema  string
}

func (c *Config) ToDBConnectionURI() string {
	query := url.Values{}

	if c.Catalog != "" {
		query.Add("catalog", c.Catalog)
	}
	if c.Schema != "" {
		query.Add("schema", c.Schema)

	}
	dsn := url.URL{
		User:     url.UserPassword("token", c.Token),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		RawQuery: query.Encode(),
		Path:     c.Path,
	}

	return strings.TrimPrefix(dsn.String(), "//")
}
