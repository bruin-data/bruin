package clickhouse

import (
	"fmt"
	"net/url"
	"strconv"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	HTTPPort int
	Secure   *int
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
	uri := url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
	}
	query := url.Values{}

	if c.HTTPPort != 0 {
		query.Set("http_port", strconv.Itoa(c.HTTPPort))
	}
	if c.Secure != nil {
		query.Set("secure", strconv.Itoa(*c.Secure))
	}

	uri.RawQuery = query.Encode()

	return uri.String()
}
