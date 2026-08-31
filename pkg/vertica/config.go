package vertica

import (
	"fmt"
	"net/url"
	"strconv"
)

type Config struct {
	Username              string
	Password              string
	Host                  string
	Port                  int
	Database              string
	Schema                string
	TLSMode               string
	ConnectionLoadBalance *int
	BackupServerNode      string
}

// optionalQuery returns the query parameters that are shared by both the native
// and the ingestr connection URIs. These map directly onto the parameters
// understood by the underlying vertica-sql-go driver.
func (c *Config) optionalQuery() url.Values {
	query := url.Values{}
	if c.TLSMode != "" {
		query.Add("tlsmode", c.TLSMode)
	}
	if c.ConnectionLoadBalance != nil {
		query.Add("connection_load_balance", strconv.Itoa(*c.ConnectionLoadBalance))
	}
	if c.BackupServerNode != "" {
		query.Add("backup_server_node", c.BackupServerNode)
	}
	return query
}

func (c *Config) ToDBConnectionURI() string {
	query := c.optionalQuery()
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
		Scheme:   "vertica",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     c.Database,
		RawQuery: c.optionalQuery().Encode(),
	}

	return u.String()
}
