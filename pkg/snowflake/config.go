package snowflake

import (
	"github.com/kelseyhightower/envconfig"
	"github.com/snowflakedb/gosnowflake"
)

type Config struct {
	Account  string `envconfig:"SNOWFLAKE_ACCOUNT"`
	Username string `envconfig:"SNOWFLAKE_USERNAME"`
	Password string `envconfig:"SNOWFLAKE_PASSWORD"`
	Region   string `envconfig:"SNOWFLAKE_REGION"`
	Role     string `envconfig:"SNOWFLAKE_ROLE"`
	Database string `envconfig:"SNOWFLAKE_DATABASE"`
	Schema   string `envconfig:"SNOWFLAKE_SCHEMA"`
}

func (c Config) DSN() (string, error) {
	snowflakeConfig := gosnowflake.Config{
		Account:  c.Account,
		User:     c.Username,
		Password: c.Password,
		Region:   c.Region,
		Role:     c.Role,
		Database: c.Database,
		Schema:   c.Schema,
	}

	return gosnowflake.DSN(&snowflakeConfig)
}

func (c Config) IsValid() bool {
	return c.Account != "" && c.Username != "" && c.Password != "" && c.Region != ""
}

func LoadConfigFromEnv() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
