package doris

import (
	"fmt"
	"net/url"
	"strings"
)

const defaultPort = 9030

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
		c.Port = defaultPort
	}

	scheme := "mysql"
	if c.Driver != "" {
		scheme += "+" + c.Driver
	} else {
		scheme += "+pymysql"
	}

	u := &url.URL{
		Scheme: scheme,
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
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
		c.Port = defaultPort
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		c.Username,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
	)

	if !strings.Contains(dsn, "?") {
		dsn += "?multiStatements=true&parseTime=true"
	} else {
		dsn += "&multiStatements=true&parseTime=true"
	}

	return dsn
}
