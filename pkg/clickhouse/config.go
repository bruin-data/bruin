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
}

func (c *Config) ToClickHouseOptions() *click_house.Options {
	return &click_house.Options{
		Addr: []string{fmt.Sprintf("%s:%d", c.Host, c.Port)},
		Auth: click_house.Auth{
			Database: c.Database,
			Username: c.Username,
			Password: c.Password,
		},
	}
}

func (c *Config) GetIngestrURI() string {
	// TODO
	return ""
}
