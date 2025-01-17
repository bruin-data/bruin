package mysql

import (
	"fmt"
	"net/url"
)

type Config struct {
	Username    string
	Password    string
	Host        string
	Port        int
	Database    string
	Driver      string
	SslCaPath   string
	SslCertPath string
	SslKeyPath  string
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

	if c.SslCaPath != "" || c.SslCertPath != "" || c.SslKeyPath != "" {
		q := u.Query()
		if c.SslCaPath != "" {
			q.Set("ssl_ca", c.SslCaPath)
		}
		if c.SslCertPath != "" {
			q.Set("ssl_cert", c.SslCertPath)
		}
		if c.SslKeyPath != "" {
			q.Set("ssl_key", c.SslKeyPath)
		}
		u.RawQuery = q.Encode()
	}

	return u.String()
}

func (c Config) ToDBConnectionURI() string {
	if c.Port == 0 {
		c.Port = 3306
	}

	hostPort := fmt.Sprintf("%s:%d", c.Host, c.Port)

	return fmt.Sprintf(
		"%s:%s@tcp(%s)/%s",
		c.Username,
		c.Password,
		hostPort,
		c.Database,
	)
}
