package trino

import (
	"fmt"
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

	userPart := c.Username
	if c.Password != "" {
		userPart = fmt.Sprintf("%s:%s", c.Username, c.Password)
	}

	baseURL := fmt.Sprintf("http://%s@%s", userPart, hostPort)
	params := url.Values{}
	if c.Catalog != "" {
		params.Set("catalog", c.Catalog)
	}
	if c.Schema != "" {
		params.Set("schema", c.Schema)
	}
	if len(params) > 0 {
		return fmt.Sprintf("%s?%s", baseURL, params.Encode())
	}
	return baseURL
}
