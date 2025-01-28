package clickhouse

import (
	"fmt"
	"strconv"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	HTTPPort string
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
	if c.HTTPPort != "" {
		return "clickhouse://" + c.Username + ":" + c.Password + "@" + c.Host + ":" + strconv.Itoa(c.Port) + "?http_port=" + c.HTTPPort
	} else {
		return "clickhouse://" + c.Username + ":" + c.Password + "@" + c.Host + ":" + strconv.Itoa(c.Port)
	}
}
