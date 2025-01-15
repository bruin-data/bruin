package appstore

import (
	"encoding/base64"
	"net/url"
)

type Config struct {
	IssuerID string
	KeyID    string
	KeyPath  string
	Key      string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("key_id", c.KeyID)
	params.Set("issuer_id", c.IssuerID)

	// ingestr will panic if neither key_path or key_base64 is provided
	switch {
	case c.KeyPath != "":
		params.Set("key_path", c.KeyPath)
	case c.Key != "":
		params.Set(
			"key_base64",
			base64.StdEncoding.EncodeToString([]byte(c.Key)),
		)
	}

	uri := url.URL{
		Scheme:   "appstore",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
