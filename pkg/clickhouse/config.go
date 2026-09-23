package clickhouse

import (
	"crypto/tls"
	"fmt"
	"maps"
	"net/url"
	"strconv"

	click_house "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/bruin-data/bruin/pkg/version"
)

const clientName = "bruin"

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Cluster  string
	HTTPPort int
	Secure   *int
	ReadOnly bool
	Settings map[string]any
}

func (c *Config) ToClickHouseOptions() *click_house.Options {
	var tlsConfig *tls.Config
	if c.Secure != nil {
		if *c.Secure == 1 {
			tlsConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}
	}
	opt := click_house.Options{
		Settings: maps.Clone(click_house.Settings(c.Settings)),
		TLS:      tlsConfig,
		Addr:     []string{fmt.Sprintf("%s:%d", c.Host, c.Port)},
		Auth: click_house.Auth{
			Database: c.Database,
			Username: c.Username,
			Password: c.Password,
		},
		ClientInfo: click_house.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: clientName, Version: version.Version},
			},
		},
	}
	if c.ReadOnly {
		if opt.Settings == nil {
			opt.Settings = click_house.Settings{}
		}
		opt.Settings["readonly"] = 1
	}
	if c.Cluster != "" {
		if opt.Settings == nil {
			opt.Settings = click_house.Settings{}
		}
		opt.Settings["distributed_ddl_output_mode"] = "throw"
		opt.Settings["distributed_ddl_task_timeout"] = 180
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

func (c *Config) GetDatabase() string {
	return c.Database
}

func (c *Config) GetCluster() string {
	return c.Cluster
}

func (c *Config) IsReadOnly() bool {
	return c.ReadOnly
}
