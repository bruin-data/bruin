package appstore

import (
	"net/url"
)

type Config struct {
	IssuerID  string
	KeyID     string
	KeyPath   string
	KeyBase64 string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("key_id", c.KeyID)
	params.Set("issuer_id", c.IssuerID)

	// ingestr will panic if neither key_path or key_base64 is provided
	switch {
	case c.KeyPath != "":
		params.Set("key_path", c.KeyPath)
	case c.KeyBase64 != "":
		params.Set("key_base64", c.KeyBase64)
	}

	uri := url.URL{
		Scheme:   "dynamodb",
		RawQuery: params.Encode(),
	}
	return uri.String()
}
