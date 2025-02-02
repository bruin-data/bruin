package clickhouse

import (
	"fmt"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	HTTPPort int
	Secure   int
}

func (c *Config) ToClickHouseOptions() *click_house.Options {
	opt := click_house.Options{
		Addr: []string{fmt.Sprintf("%s:%d", c.Host, c.Port)},
		Auth: click_house.Auth{
			Database: c.Database,
			Username: c.Username,
			Password: c.Password,
		},
	}
	return &opt
}

func (c *Config) GetIngestrURI() string {
	//nolint:nosprintfhostport
	uri := fmt.Sprintf("clickhouse://%s:%s@%s:%d", c.Username, c.Password, c.Host, c.Port)
	if c.HTTPPort != 0 {
		uri += fmt.Sprintf("?http_port=%d", c.HTTPPort)
	}
	if c.Secure != 0 {
		if c.HTTPPort != 0 {
			uri += "&"
		} else {
			uri += "?"
		}
		uri += fmt.Sprintf("secure=%d", c.Secure)
	}

	return uri
}
