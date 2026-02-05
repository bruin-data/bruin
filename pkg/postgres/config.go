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
	// CDC (Change Data Capture) configuration
	CDC         bool
	Publication string
	Slot        string
	CDCMode     string
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

func (c Config) GetIngestrURI() string {
	if c.CDC {
		return c.getCDCIngestrURI()
	}

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

func (c Config) getCDCIngestrURI() string {
	connString := fmt.Sprintf("postgres+cdc://%s:%s@%s/%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
	)

	params := url.Values{}
	if c.Publication != "" {
		params.Set("publication", c.Publication)
	}
	if c.Slot != "" {
		params.Set("slot", c.Slot)
	}
	if c.CDCMode != "" {
		params.Set("mode", c.CDCMode)
	}
	if c.SslMode != "" {
		params.Set("sslmode", c.SslMode)
	}

	if len(params) > 0 {
		connString += "?" + params.Encode()
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

	return connectionURI
}

func (c RedShiftConfig) GetIngestrURI() string {
	connString := fmt.Sprintf("redshift://%s:%s@%s/%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		c.Database,
	)

	return connString
}

func (c RedShiftConfig) GetDatabase() string {
	return c.Database
}
