package oracle

import (
	"fmt"
	"net/url"
)

type Config struct {
	//user:password@host:port/dbname
	Username string
	Password string
	Host     string
	Port     string
	DBName   string
}

func (c *Config) GetIngestrURI() string {
	url := url.URL{
		Scheme: "oracle",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%s", c.Host, c.Port),
		Path:   c.DBName,
	}
	fmt.Println(url.String())
	return url.String()
}
