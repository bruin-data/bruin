package mssql

import (
	"fmt"
	"net/url"
	"strings"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Query    string
}

func (c *Config) ToDBConnectionURI() string {
	var rawQuery string

	// If custom query parameters are provided, use them
	if c.Query != "" {
		rawQuery = c.Query
		// If database is specified but not in the query, add it
		if c.Database != "" && !strings.Contains(c.Query, "database=") {
			if strings.Contains(rawQuery, "?") || strings.Contains(rawQuery, "&") {
				rawQuery += "&database=" + url.QueryEscape(c.Database)
			} else {
				rawQuery = "database=" + url.QueryEscape(c.Database) + "&" + rawQuery
			}
		}
	} else {
		// Use default parameters for convenience (local dev, Docker, etc.)
		query := url.Values{}
		query.Add("app name", "Bruin CLI")
		query.Add("TrustServerCertificate", "true")
		query.Add("encrypt", "disable")
		if c.Database != "" {
			query.Add("database", c.Database)
		}
		rawQuery = query.Encode()
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		RawQuery: rawQuery,
	}

	return u.String()
}

func (c *Config) GetIngestrURI() string {
	rawQuery := "TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server"
	if c.Query != "" {
		rawQuery = c.Query
	}

	u := &url.URL{
		Scheme:   "mssql",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     c.Database,
		RawQuery: rawQuery,
	}

	return u.String()
}
