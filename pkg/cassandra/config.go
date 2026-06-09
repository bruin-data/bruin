package cassandra

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

const defaultPort = 9042

type Config struct {
	Username                 string
	Password                 string
	Host                     string
	Hosts                    []string
	Port                     int
	Keyspace                 string
	Consistency              string
	PageSize                 int
	Timeout                  string
	ConnectTimeout           string
	SSL                      bool
	DisableInitialHostLookup bool
}

func (c Config) GetIngestrURI() (string, error) {
	port := c.Port
	if port == 0 {
		port = defaultPort
	}

	host := strings.TrimSpace(c.Host)
	if host == "" && len(c.Hosts) > 0 {
		host = strings.TrimSpace(c.Hosts[0])
	}
	if host == "" {
		return "", errors.New("cassandra: host must be provided")
	}

	u := &url.URL{
		Scheme: "cassandra",
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   c.Keyspace,
	}

	if c.Username != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}

	q := url.Values{}
	if len(c.Hosts) > 0 {
		hosts := make([]string, 0, len(c.Hosts))
		for _, h := range c.Hosts {
			h = strings.TrimSpace(h)
			if h != "" {
				hosts = append(hosts, h)
			}
		}
		if len(hosts) > 0 {
			q.Set("hosts", strings.Join(hosts, ","))
		}
	}
	if c.Consistency != "" {
		q.Set("consistency", c.Consistency)
	}
	if c.PageSize != 0 {
		q.Set("page_size", strconv.Itoa(c.PageSize))
	}
	if c.Timeout != "" {
		q.Set("timeout", c.Timeout)
	}
	if c.ConnectTimeout != "" {
		q.Set("connect_timeout", c.ConnectTimeout)
	}
	if c.SSL {
		q.Set("ssl", "true")
	}
	if c.DisableInitialHostLookup {
		q.Set("disable_initial_host_lookup", "true")
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}
