package mssql

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

func (c *Config) ToDBConnectionURI() string {
	query := url.Values{}
	query.Add("app name", "Bruin CLI")
	if c.Database != "" {
		query.Add("database", c.Database)
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		RawQuery: query.Encode(),
	}

	return u.String()
}

func (c *Config) GetIngestrURI() string {
	u := &url.URL{
		Scheme: "mssql",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.Database,
	}

	return u.String()
}
