package sftp

import (
	"fmt"
	"net/url"
)

type Config struct {
	Host                     string
	Port                     int
	Username                 string
	Password                 string
	KeyFile                  string
	KeyPassphrase            string
	KnownHostsFile           string
	HostKeyFingerprint       []string
	InsecureSkipHostKeyCheck bool
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	if c.KeyFile != "" {
		params.Set("key_file", c.KeyFile)
	}
	if c.KeyPassphrase != "" {
		params.Set("key_passphrase", c.KeyPassphrase)
	}
	if c.KnownHostsFile != "" {
		params.Set("known_hosts_file", c.KnownHostsFile)
	}
	for _, fingerprint := range c.HostKeyFingerprint {
		params.Add("host_key_fingerprint", fingerprint)
	}
	if c.InsecureSkipHostKeyCheck {
		params.Set("insecure_skip_host_key_check", "true")
	}
	uri := url.URL{
		Scheme:   "sftp",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		RawQuery: params.Encode(),
	}
	return uri.String()
}
