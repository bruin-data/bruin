package clickhouse

import (
	"crypto/tls"
	"fmt"
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
	HTTPPort int
	Secure   *int
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
		TLS:  tlsConfig,
		Addr: []string{fmt.Sprintf("%s:%d", c.Host, c.Port)},
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
