package payrails

import (
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
)

// Config describes credentials for a Payrails connection. The mTLS material may
// be supplied as local file paths (cert_path/key_path) or as PEM content
// (cert/key); content is passed to ingestr base64-encoded so it works in
// environments without local files, such as Bruin Cloud.
type Config struct {
	ClientID     string `yaml:"client_id" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret" json:"client_secret" mapstructure:"client_secret"`
	CertPath     string `yaml:"cert_path" json:"cert_path" mapstructure:"cert_path"`
	KeyPath      string `yaml:"key_path" json:"key_path" mapstructure:"key_path"`
	Cert         string `yaml:"cert" json:"cert" mapstructure:"cert"`
	Key          string `yaml:"key" json:"key" mapstructure:"key"`
	Environment  string `yaml:"environment" json:"environment" mapstructure:"environment"`
	BaseURL      string `yaml:"base_url" json:"base_url" mapstructure:"base_url"`
}

// GetIngestrURI builds the Payrails ingestr URI.
func (c *Config) GetIngestrURI() (string, error) {
	params := url.Values{}

	clientID := strings.TrimSpace(c.ClientID)
	if clientID == "" {
		return "", errors.New("payrails: client_id must be provided")
	}
	clientSecret := strings.TrimSpace(c.ClientSecret)
	if clientSecret == "" {
		return "", errors.New("payrails: client_secret must be provided")
	}
	params.Set("client_id", clientID)
	params.Set("client_secret", clientSecret)

	switch {
	case strings.TrimSpace(c.Cert) != "":
		params.Set("cert_base64", base64.StdEncoding.EncodeToString([]byte(c.Cert)))
	case strings.TrimSpace(c.CertPath) != "":
		params.Set("cert_path", strings.TrimSpace(c.CertPath))
	default:
		return "", errors.New("payrails: cert or cert_path must be provided")
	}

	switch {
	case strings.TrimSpace(c.Key) != "":
		params.Set("key_base64", base64.StdEncoding.EncodeToString([]byte(c.Key)))
	case strings.TrimSpace(c.KeyPath) != "":
		params.Set("key_path", strings.TrimSpace(c.KeyPath))
	default:
		return "", errors.New("payrails: key or key_path must be provided")
	}

	if v := strings.TrimSpace(c.Environment); v != "" {
		params.Set("environment", v)
	}
	if v := strings.TrimSpace(c.BaseURL); v != "" {
		params.Set("base_url", v)
	}

	return "payrails://?" + params.Encode(), nil
}
