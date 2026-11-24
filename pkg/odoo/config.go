package odoo

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// maxPageSize mirrors ingestr's upper bound for page_size.
const maxPageSize = 5000

const maxPort = 65535

type Config struct {
	Host     string
	Port     int
	APIKey   string
	Database string
	Username string
	Scheme   string
	API      string
	PageSize int
}

// GetIngestrURI builds an odoo://<host>?api_key=... URI. Empty optional fields are left out so that
// ingestr applies its own defaults (https, automatic API detection, 1000 records per page).
func (c *Config) GetIngestrURI() (string, error) {
	host := strings.TrimSuffix(strings.TrimSpace(c.Host), "/")
	if host == "" {
		return "", errors.New("odoo: host must be provided")
	}
	if strings.Contains(host, "://") {
		return "", fmt.Errorf("odoo: host must not include a scheme, got %q; set scheme to http or https instead", host)
	}

	// The key goes into the URI exactly as configured: credential masking registers the configured
	// value, so a trimmed copy would leak in debug output.
	if strings.TrimSpace(c.APIKey) == "" {
		return "", errors.New("odoo: api_key must be provided")
	}

	database := strings.TrimSpace(c.Database)
	username := strings.TrimSpace(c.Username)

	scheme := strings.TrimSpace(c.Scheme)
	if scheme != "" && scheme != "https" && scheme != "http" {
		return "", fmt.Errorf("odoo: scheme must be https or http, got %q", scheme)
	}

	api := strings.TrimSpace(c.API)
	switch api {
	case "", "auto", "json2":
	case "jsonrpc":
		if database == "" || username == "" {
			return "", errors.New("odoo: database and username are required for the jsonrpc API (Odoo 18 and older)")
		}
	default:
		return "", fmt.Errorf("odoo: api must be one of auto, json2, jsonrpc, got %q", api)
	}

	if c.PageSize < 0 || c.PageSize > maxPageSize {
		return "", fmt.Errorf("odoo: page_size must be between 1 and %d, got %d", maxPageSize, c.PageSize)
	}

	if c.Port < 0 || c.Port > maxPort {
		return "", fmt.Errorf("odoo: port must be between 1 and %d, got %d", maxPort, c.Port)
	}

	// The host may carry a port and a path prefix, e.g. erp.example.com:8069/odoo.
	u := &url.URL{Scheme: "odoo", Host: host}
	if i := strings.Index(host, "/"); i >= 0 {
		u.Host = host[:i]
		u.Path = host[i:]
	}

	// Ingestr has no port parameter, so a separate port goes into the authority, ahead of any path prefix.
	if c.Port != 0 {
		hostname, hasPort, err := splitHostPort(u.Host)
		if err != nil {
			return "", err
		}
		if hasPort {
			return "", fmt.Errorf("odoo: host %q already includes a port; set the port in either host or port, not both", host)
		}
		u.Host = net.JoinHostPort(hostname, strconv.Itoa(c.Port))
	}

	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if database != "" {
		params.Set("database", database)
	}
	if username != "" {
		params.Set("username", username)
	}
	if scheme != "" {
		params.Set("scheme", scheme)
	}
	if api != "" {
		params.Set("api", api)
	}
	if c.PageSize > 0 {
		params.Set("page_size", strconv.Itoa(c.PageSize))
	}
	u.RawQuery = params.Encode()

	return u.String(), nil
}

// splitHostPort returns the hostname of an authority and whether it carries a port. Unlike
// net.SplitHostPort it accepts an authority without a port, and it reads an unbracketed IPv6
// literal such as ::1 as a hostname.
func splitHostPort(authority string) (string, bool, error) {
	if strings.HasPrefix(authority, "[") {
		hostname, rest, closed := strings.Cut(authority[1:], "]")
		if !closed || (rest != "" && !strings.HasPrefix(rest, ":")) {
			return "", false, fmt.Errorf("odoo: host %q is not a valid bracketed IPv6 address", authority)
		}
		return hostname, rest != "", nil
	}

	if strings.Count(authority, ":") == 1 {
		hostname, _, _ := strings.Cut(authority, ":")
		return hostname, true, nil
	}

	return authority, false, nil
}
