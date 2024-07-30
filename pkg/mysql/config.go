package mysql

import (
	"fmt"
	"net/url"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Driver   string
}

func (c Config) GetIngestrURI() string {
	if c.Port == 0 {
		c.Port = 3306
	}
	host := fmt.Sprintf("%s:%d", c.Host, c.Port)
	scheme := "mysql"
	if c.Driver != "" {
		scheme += "+" + c.Driver
	}

	u := &url.URL{
		Scheme: scheme,
		User:   url.UserPassword(c.Username, c.Password),
		Host:   host,
		Path:   c.Database,
	}

	return u.String()
}

func (c Config) ToDBConnectionURI() string {
	u := &url.URL{
		User: url.UserPassword(c.Username, c.Password),
		Host: c.Host,
		Path: c.Database,
	}

	return u.String()
}
