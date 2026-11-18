package azureblob

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	AccountName      string
	ConnectionString string
	AccountKey       string
	SASToken         string
	TenantID         string
	ClientID         string
	ClientSecret     string
	ContainerName    string
	PathToFile       string
	Layout           string
}

// az://<container_name>/<path_to_file>?account_name=<account_name>&...
// The container and path only matter when the connection is an ingestr destination.
func (c Config) GetIngestrURI() (string, error) {
	accountName := strings.TrimSpace(c.AccountName)
	if accountName == "" && strings.TrimSpace(c.ConnectionString) == "" {
		return "", errors.New("azure_blob: account_name or connection_string must be provided")
	}

	// Credentials go in unchanged: the log masker only knows their configured form.
	params := url.Values{}
	addOptionalParam(params, "account_name", accountName)
	addOptionalParam(params, "connection_string", c.ConnectionString)
	addOptionalParam(params, "account_key", c.AccountKey)
	addOptionalParam(params, "sas_token", c.SASToken)
	addOptionalParam(params, "tenant_id", c.TenantID)
	addOptionalParam(params, "client_id", c.ClientID)
	addOptionalParam(params, "client_secret", c.ClientSecret)
	addOptionalParam(params, "layout", strings.TrimSpace(c.Layout))

	container := strings.Trim(strings.TrimSpace(c.ContainerName), "/")
	path := strings.Trim(strings.TrimSpace(c.PathToFile), "/")
	if container == "" && path != "" {
		return "", errors.New("azure_blob: path_to_file requires container_name")
	}

	uri := url.URL{Scheme: "az", Host: container, RawQuery: params.Encode()}
	if path != "" {
		uri.Path = "/" + path
	}
	if container == "" {
		// url.URL drops the "//" without a host; ingestr expects az://?...
		return "az://?" + uri.RawQuery, nil
	}

	return uri.String(), nil
}

func addOptionalParam(params url.Values, key, value string) {
	if value == "" {
		return
	}

	params.Set(key, value)
}
