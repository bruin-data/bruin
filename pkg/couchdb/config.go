package couchdb

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	SSL      bool
}

// GetIngestrURI builds a couchdb:// (or couchdb+https:// when SSL is enabled) URI.
// The database is selected per asset via source_table, so it is not part of the URI.
func (c *Config) GetIngestrURI() (string, error) {
	host := strings.TrimSpace(c.Host)
	if host == "" {
		return "", errors.New("couchdb: host must be provided")
	}
	if c.Port < 0 || c.Port > 65535 {
		return "", fmt.Errorf("couchdb: port must be between 1 and 65535, got %d", c.Port)
	}

	scheme := "couchdb"
	if c.SSL {
		scheme = "couchdb+https"
	}

	// Accept IPv6 hosts with or without brackets; they must be bracketed in the URI.
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	u := &url.URL{
		Scheme: scheme,
		Host:   host,
	}
	switch {
	case c.Port != 0:
		u.Host = net.JoinHostPort(host, strconv.Itoa(c.Port))
	case strings.Contains(host, ":"):
		u.Host = "[" + host + "]"
	}

	if c.Username != "" || c.Password != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}

	return u.String(), nil
}
