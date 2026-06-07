package cratedb

import (
	"net"
	"net/url"
	"strconv"
)

const defaultPort = 5432

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	SslMode  string
}

func (c Config) GetIngestrURI() string {
	u := &url.URL{
		Scheme: "cratedb",
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.getPort())),
	}

	if c.Password != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	} else if c.Username != "" {
		u.User = url.User(c.Username)
	}

	if c.SslMode != "" {
		query := url.Values{}
		query.Set("sslmode", c.SslMode)
		u.RawQuery = query.Encode()
	}

	return u.String()
}

func (c Config) getPort() int {
	if c.Port == 0 {
		return defaultPort
	}

	return c.Port
}
