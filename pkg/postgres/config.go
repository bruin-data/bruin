package postgres

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
)

type Config struct {
	Username     string
	Password     string
	Host         string
	Port         int
	Database     string
	Schema       string
	PoolMaxConns int
	SslMode      string
}

// ToDBConnectionURI returns a connection URI to be used with the pgx package.
func (c Config) ToDBConnectionURI() string {
	connectionURI := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s&pool_max_conns=%d",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
		c.SslMode,
		c.PoolMaxConns,
	)
	if c.Schema != "" {
		connectionURI += "&search_path=" + c.Schema
	}

	return connectionURI
}

func (c Config) ToIngestrUrl() string {
	connString := fmt.Sprintf("postgresql://%s:%s@%s/%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
	)

	if c.SslMode != "" {
		connString += "?sslmode=" + c.SslMode
	}

	return connString
}
