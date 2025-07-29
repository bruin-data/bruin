package trino

import (
	"fmt"
	"net/url"
)

type Config struct {
	Username string // Required
	Host     string // Required
	Port     int    // Required
	Catalog  string // Optional - can be empty to use default catalog
	Schema   string // Optional - can be empty to use default schema
}

func (c Config) ToDSN() string {
	baseURL := fmt.Sprintf("http://%s@%s:%d", c.Username, c.Host, c.Port)
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
