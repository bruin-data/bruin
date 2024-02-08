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
	schemaParam := ""
	if c.Schema != "" {
		schemaParam = fmt.Sprintf("&search_path=%s", c.Schema)
	}

	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s&pool_max_conns=%d%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
		c.SslMode,
		c.PoolMaxConns,
		schemaParam,
	)
}
