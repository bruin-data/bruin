package onelake

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	WorkspaceName             string
	LakehouseName             string
	TenantID                  string
	ClientID                  string
	ClientSecret              string
	SASToken                  string
	UseAzureDefaultCredential bool
}

// GetIngestrURI builds the URI ingestr expects for a Microsoft OneLake
// destination. OneLake requires Microsoft Entra ID authentication, so one of
// the following must be provided: a service principal (client_id/client_secret,
// optionally tenant_id), a SAS token, or use_azure_default_credential which lets
// ingestr fall back to DefaultAzureCredential (env vars, managed identity, or
// Azure CLI login).
//
//	onelake://<workspace>/<lakehouse>?tenant_id=<tenant_id>&client_id=<client_id>&client_secret=<client_secret>
func (c Config) GetIngestrURI() (string, error) {
	workspace := strings.TrimSpace(c.WorkspaceName)
	lakehouse := strings.TrimSpace(c.LakehouseName)
	if workspace == "" || lakehouse == "" {
		return "", errors.New("onelake: both workspace_name and lakehouse_name must be provided")
	}

	query := url.Values{}
	switch {
	case c.ClientID != "":
		query.Set("client_id", c.ClientID)
		if c.ClientSecret != "" {
			query.Set("client_secret", c.ClientSecret)
		}
		if c.TenantID != "" {
			query.Set("tenant_id", c.TenantID)
		}
	case c.SASToken != "":
		query.Set("sas_token", c.SASToken)
	case c.UseAzureDefaultCredential:
		// No auth params: ingestr falls back to DefaultAzureCredential.
	default:
		return "", errors.New("onelake: authentication required: set client_id/client_secret/tenant_id, sas_token, or use_azure_default_credential")
	}

	u := url.URL{
		Scheme:   "onelake",
		Host:     workspace,
		Path:     "/" + lakehouse,
		RawQuery: query.Encode(),
	}

	return u.String(), nil
}
