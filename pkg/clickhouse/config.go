package clickhouse

import click_house "github.com/ClickHouse/clickhouse-go/v2"

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
}

func (c *Config) ToClickHouseAuth() click_house.Auth {
	// TODO
	return click_house.Auth{}
}

func (c *Config) GetIngestrURI() string {
	// TODO
	return ""
}
