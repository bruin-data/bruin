package adls

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	AccountName  string
	TenantID     string
	ClientID     string
	ClientSecret string
	AccountKey   string
	SASToken     string
	Layout       string
}

func (c Config) GetIngestrURI() (string, error) {
	accountName := strings.TrimSpace(c.AccountName)
	if accountName == "" {
		return "", errors.New("adls: account_name must be provided")
	}

	params := url.Values{}
	params.Set("account_name", accountName)
	addOptionalParam(params, "tenant_id", c.TenantID)
	addOptionalParam(params, "client_id", c.ClientID)
	addOptionalParam(params, "client_secret", c.ClientSecret)
	addOptionalParam(params, "account_key", c.AccountKey)
	addOptionalParam(params, "sas_token", c.SASToken)

	layout := strings.TrimSpace(c.Layout)
	if layout != "" {
		params.Set("layout", layout)
	}

	return "adls://?" + params.Encode(), nil
}

func addOptionalParam(params url.Values, key, value string) {
	if value == "" {
		return
	}

	params.Set(key, value)
}
