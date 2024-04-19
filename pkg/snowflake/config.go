package snowflake

import (
	"github.com/snowflakedb/gosnowflake"
	"net/url"
)

type Config struct {
	Account   string `envconfig:"SNOWFLAKE_ACCOUNT"`
	Username  string `envconfig:"SNOWFLAKE_USERNAME"`
	Password  string `envconfig:"SNOWFLAKE_PASSWORD"`
	Region    string `envconfig:"SNOWFLAKE_REGION"`
	Role      string `envconfig:"SNOWFLAKE_ROLE"`
	Database  string `envconfig:"SNOWFLAKE_DATABASE"`
	Schema    string `envconfig:"SNOWFLAKE_SCHEMA"`
	Warehouse string `envconfig:"SNOWFLAKE_WAREHOUSE"`
}

func (c Config) DSN() (string, error) {
	snowflakeConfig := gosnowflake.Config{
		Account:   c.Account,
		User:      c.Username,
		Password:  c.Password,
		Region:    c.Region,
		Role:      c.Role,
		Database:  c.Database,
		Schema:    c.Schema,
		Warehouse: c.Warehouse,
	}

	return gosnowflake.DSN(&snowflakeConfig)
}

func (c Config) GetIngestrURI() (string, error) {
	u := &url.URL{
		Scheme: "snowflake",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   c.Account,
		Path:   c.Database,
	}

	values := u.Query()
	if c.Warehouse != "" {
		values.Add("warehouse", c.Warehouse)
	}

	if c.Role != "" {
		values.Add("role", c.Role)
	}

	u.RawQuery = values.Encode()

	return u.String(), nil
}

func (c Config) IsValid() bool {
	return c.Account != "" && c.Username != "" && c.Password != "" && c.Region != ""
}
