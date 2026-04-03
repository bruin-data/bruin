package rabbitmq

import (
	"fmt"
	"net/url"
)

type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
	TLS      bool
}

func (c *Config) GetIngestrURI() string {
	host := c.Host
	if host == "" {
		host = "localhost"
	}

	scheme := "amqp"
	defaultPort := 5672
	if c.TLS {
		scheme = "amqps"
		defaultPort = 5671
	}

	port := c.Port
	if port == 0 {
		port = defaultPort
	}

	userInfo := url.UserPassword(c.Username, c.Password)

	vhost := c.Vhost
	if vhost == "" {
		vhost = "/"
	}

	u := &url.URL{
		Scheme: scheme,
		User:   userInfo,
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   vhost,
	}

	return u.String()
}
