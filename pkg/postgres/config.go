package postgres

import (
	"fmt"
	"net"
	"strconv"
)

type Config struct {
	Username     string
	Password     string
	Host         string
	Port         int
	Database     string
	PoolMaxConns int
	SslMode      string
}

// ToDBConnectionURI returns a connection URI to be used with the pgx package.
func (c Config) ToDBConnectionURI() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s&pool_max_conns=%d",
		c.Username,
		c.Password,
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
		c.SslMode,
		c.PoolMaxConns,
	)
}
