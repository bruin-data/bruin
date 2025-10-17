package postgres

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
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
	ReadOnly     bool
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
	if c.ReadOnly {
		connectionURI += "&options=" + url.QueryEscape("-c default_transaction_read_only=on")
	}

	return connectionURI
}

func (c Config) GetIngestrURI() string {
	connString := fmt.Sprintf("postgresql://%s:%s@%s/%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
	)

	params := []string{}
	if c.SslMode != "" {
		params = append(params, "sslmode="+c.SslMode)
	}
	if c.ReadOnly {
		params = append(params, "options="+url.QueryEscape("-c default_transaction_read_only=on"))
	}

	if len(params) > 0 {
		connString += "?" + strings.Join(params, "&")
	}

	return connString
}

func (c Config) GetDatabase() string {
	return c.Database
}

type RedShiftConfig struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Schema   string
	SslMode  string
	ReadOnly bool
}

// ToDBConnectionURI returns a connection URI to be used with the pgx package.
func (c RedShiftConfig) ToDBConnectionURI() string {
	connectionURI := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
		c.SslMode,
	)
	if c.Schema != "" {
		connectionURI += "&search_path=" + c.Schema
	}
	if c.ReadOnly {
		connectionURI += "&options=" + url.QueryEscape("-c default_transaction_read_only=on")
	}

	return connectionURI
}

func (c RedShiftConfig) GetIngestrURI() string {
	connString := fmt.Sprintf("redshift://%s:%s@%s/%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
	)

	params := []string{}
	if c.SslMode != "" {
		params = append(params, "sslmode="+c.SslMode)
	}
	if c.ReadOnly {
		params = append(params, "options="+url.QueryEscape("-c default_transaction_read_only=on"))
	}

	if len(params) > 0 {
		connString += "?" + strings.Join(params, "&")
	}

	return connString
}

func (c RedShiftConfig) GetDatabase() string {
	return c.Database
}
