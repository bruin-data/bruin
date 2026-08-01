package fabric

import (
	"errors"
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

// GetIngestrURI builds the URI ingestr expects for a Microsoft Fabric Warehouse
// source/destination. Fabric only supports Microsoft Entra ID authentication, so a
// service-principal URI is produced when client credentials are set, falling back to
// ActiveDirectoryDefault when use_azure_default_credential is enabled. SQL
// username/password auth is rejected because Fabric Warehouse has no such login.
//
//	fabric://<client_id>:<client_secret>@<host>:<port>/<database>?tenant_id=<tenant_id>
func (c *Config) GetIngestrURI() (string, error) {
	if c.ClientID == "" && !c.UseAzureDefaultCredential {
		return "", errors.New("fabric ingestr assets require Microsoft Entra ID authentication: set client_id/client_secret/tenant_id or use_azure_default_credential")
	}

	u := &url.URL{
		Scheme: "fabric",
		Host:   fmt.Sprintf("%s:%d", c.Host, c.getPort()),
		Path:   "/" + c.Database,
	}

	query := url.Values{}
	if c.ClientID != "" {
		if c.ClientSecret != "" {
			u.User = url.UserPassword(c.ClientID, c.ClientSecret)
		} else {
			u.User = url.User(c.ClientID)
		}
		if c.TenantID != "" {
			query.Set("tenant_id", c.TenantID)
		}
	} else {
		query.Set("fedauth", "ActiveDirectoryDefault")
	}

	u.RawQuery = query.Encode()

	return u.String(), nil
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
