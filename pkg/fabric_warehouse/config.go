package fabric_warehouse

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/microsoft/go-mssqldb/azuread"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Options  string

	UseAzureDefaultCredential bool
	ClientID                  string
	ClientSecret              string
	TenantID                  string
}

func (c *Config) DriverName() string {
	if c.usesAzureAD() {
		return azuread.DriverName
	}

	return "sqlserver"
}

func (c *Config) ToDBConnectionURI() string {
	username := c.Username
	password := c.Password

	rawQuery := c.Options
	if rawQuery == "" {
		query := url.Values{}
		query.Add("app name", "Bruin CLI")
		query.Add("TrustServerCertificate", "false")
		query.Add("encrypt", "true")
		rawQuery = query.Encode()
	}

	if c.Database != "" && !strings.Contains(rawQuery, "database=") {
		rawQuery = appendQueryParam(rawQuery, "database="+url.QueryEscape(c.Database))
	}

	if c.UseAzureDefaultCredential && !strings.Contains(rawQuery, "fedauth=") {
		rawQuery = appendQueryParam(rawQuery, "fedauth=ActiveDirectoryDefault")
	}

	if c.ClientID != "" {
		if !strings.Contains(rawQuery, "fedauth=") {
			rawQuery = appendQueryParam(rawQuery, "fedauth=ActiveDirectoryServicePrincipal")
		}

		username = c.ClientID
		if c.TenantID != "" {
			username = fmt.Sprintf("%s@%s", c.ClientID, c.TenantID)
		}
		if password == "" {
			password = c.ClientSecret
		}
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		Host:     fmt.Sprintf("%s:%d", c.Host, c.getPort()),
		RawQuery: rawQuery,
	}

	if username != "" || password != "" {
		if password != "" {
			u.User = url.UserPassword(username, password)
		} else {
			u.User = url.User(username)
		}
	}

	return u.String()
}

func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf(
		"fabric://?server=%s&database=%s&authentication=sql&username=%s&password=%s",
		c.Host, c.Database, c.Username, c.Password,
	)
}

func (c *Config) getPort() int {
	if c.Port == 0 {
		return 1433
	}
	return c.Port
}

func (c *Config) usesAzureAD() bool {
	if c.UseAzureDefaultCredential || c.ClientID != "" {
		return true
	}

	return strings.Contains(strings.ToLower(c.Options), "fedauth=")
}

func appendQueryParam(rawQuery, param string) string {
	if rawQuery == "" {
		return param
	}
	if strings.HasSuffix(rawQuery, "&") || strings.HasSuffix(rawQuery, "?") {
		return rawQuery + param
	}
	if strings.Contains(rawQuery, "?") {
		return rawQuery + "&" + param
	}
	return rawQuery + "&" + param
}
