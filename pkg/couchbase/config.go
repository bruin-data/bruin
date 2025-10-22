package couchbase

import (
	"net/url"
)

type Config struct {
	Username string
	Password string
	Host     string
	Bucket   string
	SSL      bool
}

func (c *Config) GetIngestrURI() string {
	scheme := "couchbase"

	u := &url.URL{
		Scheme: scheme,
		Host:   c.Host,
	}

	if c.Username != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}

	if c.Bucket != "" {
		u.Path = c.Bucket
	}

	if c.SSL {
		q := u.Query()
		q.Set("ssl", "true")
		u.RawQuery = q.Encode()
	}

	return u.String()
}
