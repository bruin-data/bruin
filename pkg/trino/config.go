package trino

import (
	"net"
	"net/url"
	"strconv"
)

type Config struct {
	Username string // Required
	Password string // Optional
	Host     string // Required
	Port     int    // Required
	Catalog  string // Optional - can be empty to use default catalog
	Schema   string // Optional - can be empty to use default schema
}

func (c Config) ToDSN() string {
	hostPort := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))

	user := url.User(c.Username)
	if c.Password != "" {
		user = url.UserPassword(c.Username, c.Password)
	}

	params := url.Values{}
	if c.Catalog != "" {
		params.Set("catalog", c.Catalog)
	}
	if c.Schema != "" {
		params.Set("schema", c.Schema)
	}
	u := &url.URL{
		Scheme:   "http",
		Host:     hostPort,
		User:     user,
		RawQuery: params.Encode(),
	}
	return u.String()
}

func (c Config) GetIngestrURI() string {
	hostPort := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))

	u := &url.URL{
		Scheme: "trino",
		Host:   hostPort,
	}
	if c.Username != "" {
		if c.Password != "" {
			u.User = url.UserPassword(c.Username, c.Password)
		} else {
			u.User = url.User(c.Username)
		}
	}
	if c.Catalog != "" {
		u.Path = "/" + c.Catalog
	}

	return u.String()
}
