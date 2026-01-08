package databricks

import (
	"fmt"
	"net/url"
	"strings"
)

type Config struct {
	// Token is the Databricks Personal API token (used for PAT authentication)
	Token string
	Host  string
	Port  int
	// Path will determine cluster or sql warehouse. See https://docs.databricks.com/en/dev-tools/go-sql-driver.html#connect-with-a-dsn-connection-string
	Path    string
	Catalog string
	Schema  string
	// ClientID and ClientSecret are used for OAuth M2M (machine-to-machine) authentication
	// When both are provided, OAuth M2M authentication is used instead of PAT
	ClientID     string
	ClientSecret string
}

// UseOAuthM2M returns true if OAuth M2M authentication should be used
func (c *Config) UseOAuthM2M() bool {
	return c.ClientID != "" && c.ClientSecret != ""
}

func (c *Config) ToDBConnectionURI() string {
	query := url.Values{}

	if c.Catalog != "" {
		query.Add("catalog", c.Catalog)
	}
	if c.Schema != "" {
		query.Add("schema", c.Schema)
	}

	// Use OAuth M2M authentication if client credentials are provided
	// DSN format: <hostname>:<port>/<path>?authType=OAuthM2M&clientID=<id>&clientSecret=<secret>
	if c.UseOAuthM2M() {
		query.Add("authType", "OAuthM2M")
		query.Add("clientID", c.ClientID)
		query.Add("clientSecret", c.ClientSecret)

		dsn := url.URL{
			Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
			RawQuery: query.Encode(),
			Path:     c.Path,
		}

		return strings.TrimPrefix(dsn.String(), "//")
	}

	// Use PAT (Personal Access Token) authentication
	dsn := url.URL{
		User:     url.UserPassword("token", c.Token),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		RawQuery: query.Encode(),
		Path:     c.Path,
	}

	return strings.TrimPrefix(dsn.String(), "//")
}

// GetIngestrURI returns the connection URI for ingestr.
// For PAT: databricks://token:<access_token>@<server_hostname>?http_path=<http_path>&catalog=<catalog>&schema=<schema>
// For OAuth M2M: databricks://<server_hostname>?http_path=<http_path>&catalog=<catalog>&schema=<schema>&client_id=<id>&client_secret=<secret>
// See: https://bruin-data.github.io/ingestr/supported-sources/databricks.html
func (c *Config) GetIngestrURI() string {
	query := url.Values{}

	if c.Path != "" {
		query.Add("http_path", c.Path)
	}
	if c.Catalog != "" {
		query.Add("catalog", c.Catalog)
	}
	if c.Schema != "" {
		query.Add("schema", c.Schema)
	}

	// Use OAuth M2M authentication if client credentials are provided
	if c.UseOAuthM2M() {
		query.Add("client_id", c.ClientID)
		query.Add("client_secret", c.ClientSecret)

		u := &url.URL{
			Scheme:   "databricks",
			Host:     c.Host,
			RawQuery: query.Encode(),
		}

		return u.String()
	}

	// Use PAT (Personal Access Token) authentication
	u := &url.URL{
		Scheme:   "databricks",
		User:     url.UserPassword("token", c.Token),
		Host:     c.Host,
		RawQuery: query.Encode(),
	}

	return u.String()
}
