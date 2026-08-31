package twenty

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Config holds the settings required to connect to a Twenty CRM workspace.
type Config struct {
	// Host is the workspace host, e.g. api.twenty.com for Twenty Cloud or your own
	// domain for a self-hosted instance. Required.
	Host string
	// APIKey is created in the workspace under Settings > API & Webhooks. Required.
	APIKey string
	// Scheme is the transport used to reach the workspace, https or http.
	// Optional, defaults to https.
	Scheme string
	// BasePath is where the REST API is mounted. Optional, defaults to /rest.
	BasePath string
	// PageSize is the number of rows per request. Optional, defaults to and is
	// capped at 200.
	PageSize *int
	// RateLimit is the number of requests per second. Optional, defaults to 1.33
	// (80% of Twenty's documented 100 requests/minute).
	RateLimit *float64
	// IncludeDeleted controls whether a second pass re-reads soft-deleted records.
	// Optional, defaults to true.
	IncludeDeleted *bool
}

// GetIngestrURI builds the ingestr source URI for Twenty CRM.
// Format: twenty://<host>?api_key=<api_key>[&scheme=&base_path=&page_size=&rate_limit=&include_deleted=]
func (c *Config) GetIngestrURI() (string, error) {
	host := strings.TrimSpace(c.Host)
	if host == "" {
		return "", errors.New("twenty: host must be provided")
	}
	apiKey := strings.TrimSpace(c.APIKey)
	if apiKey == "" {
		return "", errors.New("twenty: api_key must be provided")
	}

	params := url.Values{}
	params.Set("api_key", apiKey)

	if scheme := strings.TrimSpace(c.Scheme); scheme != "" {
		params.Set("scheme", scheme)
	}
	if basePath := strings.TrimSpace(c.BasePath); basePath != "" {
		params.Set("base_path", basePath)
	}
	if c.PageSize != nil {
		if *c.PageSize <= 0 {
			return "", fmt.Errorf("twenty: page_size must be a positive integer, got %d", *c.PageSize)
		}
		params.Set("page_size", strconv.Itoa(*c.PageSize))
	}
	if c.RateLimit != nil {
		if *c.RateLimit <= 0 {
			return "", fmt.Errorf("twenty: rate_limit must be a positive number, got %v", *c.RateLimit)
		}
		params.Set("rate_limit", strconv.FormatFloat(*c.RateLimit, 'f', -1, 64))
	}
	if c.IncludeDeleted != nil {
		params.Set("include_deleted", strconv.FormatBool(*c.IncludeDeleted))
	}

	uri := url.URL{
		Scheme:   "twenty",
		Host:     host,
		RawQuery: params.Encode(),
	}

	return uri.String(), nil
}
