package onelake

import (
	"errors"
	"fmt"
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
// destination. OneLake requires Microsoft Entra ID authentication, so exactly
// one of the following authentication modes must be fully configured:
//   - Service principal: tenant_id + client_id + client_secret (all three).
//   - SAS token: sas_token.
//   - DefaultAzureCredential: use_azure_default_credential, which lets ingestr
//     authenticate via env vars, a managed identity, or the Azure CLI login.
//
// Partially configured modes (e.g. client_id without client_secret) and
// combinations of more than one mode are rejected so misconfigurations surface
// as clear errors instead of silently producing an unusable URI.
//
//	onelake://<workspace>/<lakehouse>?tenant_id=<tenant_id>&client_id=<client_id>&client_secret=<client_secret>
func (c Config) GetIngestrURI() (string, error) {
	workspace := strings.TrimSpace(c.WorkspaceName)
	lakehouse := strings.TrimSpace(c.LakehouseName)
	if workspace == "" || lakehouse == "" {
		return "", errors.New("onelake: both workspace_name and lakehouse_name must be provided")
	}

	// Detect which auth modes have any field set so we can reject partial or
	// ambiguous configurations.
	servicePrincipalSet := c.TenantID != "" || c.ClientID != "" || c.ClientSecret != ""
	sasSet := c.SASToken != ""

	modesConfigured := 0
	for _, set := range []bool{servicePrincipalSet, sasSet, c.UseAzureDefaultCredential} {
		if set {
			modesConfigured++
		}
	}

	switch {
	case modesConfigured == 0:
		return "", errors.New("onelake: authentication required: set service principal (tenant_id + client_id + client_secret), sas_token, or use_azure_default_credential")
	case modesConfigured > 1:
		return "", errors.New("onelake: multiple authentication methods configured; provide exactly one of service principal (tenant_id + client_id + client_secret), sas_token, or use_azure_default_credential")
	}

	query := url.Values{}
	switch {
	case servicePrincipalSet:
		var missing []string
		if c.TenantID == "" {
			missing = append(missing, "tenant_id")
		}
		if c.ClientID == "" {
			missing = append(missing, "client_id")
		}
		if c.ClientSecret == "" {
			missing = append(missing, "client_secret")
		}
		if len(missing) > 0 {
			return "", fmt.Errorf("onelake: service principal authentication requires %s", strings.Join(missing, ", "))
		}
		query.Set("tenant_id", c.TenantID)
		query.Set("client_id", c.ClientID)
		query.Set("client_secret", c.ClientSecret)
	case sasSet:
		query.Set("sas_token", c.SASToken)
	case c.UseAzureDefaultCredential:
		// No auth params: ingestr falls back to DefaultAzureCredential.
	}

	u := url.URL{
		Scheme:   "onelake",
		Host:     workspace,
		Path:     "/" + lakehouse,
		RawQuery: query.Encode(),
	}

	return u.String(), nil
}
