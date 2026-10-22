package quack

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type Config struct {
	Host       string
	Port       int
	Token      string
	DisableSSL *bool
}

func (c *Config) GetIngestrURI() (string, error) {
	host := strings.TrimSpace(c.Host)
	if host == "" {
		return "", errors.New("quack: host must be provided")
	}
	if c.Port < 0 || c.Port > 65535 {
		return "", fmt.Errorf("quack: port must be between 1 and 65535, got %d", c.Port)
	}
	switch {
	case c.Port != 0:
		host = net.JoinHostPort(host, strconv.Itoa(c.Port))
	case strings.Contains(host, ":"):
		host = "[" + host + "]"
	}

	params := url.Values{}
	if c.Token != "" {
		params.Set("token", c.Token)
	}
	if c.DisableSSL != nil {
		params.Set("disable_ssl", strconv.FormatBool(*c.DisableSSL))
	}
	uri := url.URL{
		Scheme:   "quack",
		Host:     host,
		RawQuery: params.Encode(),
	}
	return uri.String(), nil
}
