package abraflexi

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Config holds the settings required to connect to an ABRA Flexi (Flexibee)
// account through its REST API.
type Config struct {
	// Host is the Flexi account host, e.g. example.flexibee.eu. Required.
	Host string
	// Path is an optional URL path prefix in front of the REST API, used by
	// self-hosted installs mounted under a sub-path (e.g. /flexi). Cloud Flexi
	// leaves it empty.
	Path string
	// Username is the Flexi API user. Required.
	Username string
	// Password is that user's password. Required.
	Password string
	// Company is the company database code as it appears in the REST path, e.g.
	// acme_s_r_o_. It selects which set of books to read and is never defaulted.
	// Required.
	Company string
	// Scheme is the transport used to reach the account, https or http. Optional,
	// defaults to https (http is only accepted by ingestr for loopback hosts).
	Scheme string
	// PageSize is the number of rows per request. Optional, defaults to 1000.
	PageSize *int
	// RateLimit is the number of requests per second. Optional, defaults to 4.
	RateLimit *float64
	// IncludeExpensive controls whether properties Flexi flags as expensive to
	// compute are included. Optional, defaults to true.
	IncludeExpensive *bool
}

type requiredField struct {
	key   string
	value string
}

// GetIngestrURI builds the ingestr source URI for ABRA Flexi.
// Format: abra://<host>?username=<user>&password=<password>&company=<company>[&scheme=&page_size=&rate_limit=&include_expensive=]
func (c *Config) GetIngestrURI() (string, error) {
	host := strings.TrimSpace(c.Host)
	if host == "" {
		return "", errors.New("abraflexi: host must be provided")
	}

	requiredFields := []requiredField{
		{"username", c.Username},
		{"password", c.Password},
		{"company", c.Company},
	}

	params := url.Values{}
	for _, field := range requiredFields {
		value := strings.TrimSpace(field.value)
		if value == "" {
			return "", fmt.Errorf("abraflexi: %s must be provided", field.key)
		}
		params.Set(field.key, value)
	}

	if scheme := strings.TrimSpace(c.Scheme); scheme != "" {
		params.Set("scheme", scheme)
	}
	if c.PageSize != nil {
		if *c.PageSize <= 0 {
			return "", fmt.Errorf("abraflexi: page_size must be a positive integer, got %d", *c.PageSize)
		}
		params.Set("page_size", strconv.Itoa(*c.PageSize))
	}
	if c.RateLimit != nil {
		if *c.RateLimit <= 0 {
			return "", fmt.Errorf("abraflexi: rate_limit must be a positive number, got %v", *c.RateLimit)
		}
		params.Set("rate_limit", strconv.FormatFloat(*c.RateLimit, 'f', -1, 64))
	}
	if c.IncludeExpensive != nil {
		params.Set("include_expensive", strconv.FormatBool(*c.IncludeExpensive))
	}

	uri := url.URL{
		Scheme:   "abra",
		Host:     host,
		RawQuery: params.Encode(),
	}

	if path := strings.TrimSpace(c.Path); path != "" {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		uri.Path = strings.TrimSuffix(path, "/")
	}

	return uri.String(), nil
}
