package emr_serverless

import (
	"fmt"
	"reflect"
)

type Config struct {
	AccessKey     string `yaml:"access_key"`
	SecretKey     string `yaml:"secret_key"`
	ApplicationID string `yaml:"application_id"`
	ExecutionRole string `yaml:"execution_role"`
	Region        string `yaml:"region"`
}

func (c *Config) validate() error {
	typ := reflect.TypeOf(c).Elem()
	val := reflect.ValueOf(c).Elem()
	for field := range typ.NumField() {
		fieldName := typ.Field(field).Tag.Get("yaml")
		fieldValue := val.Field(field).String()
		if fieldValue == "" {
			return fmt.Errorf("missing required field: %s", fieldName)
		}
	}
	return nil
}

type Client struct {
	Config
}

func NewClient(c Config) (*Client, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &Client{
		Config: c,
	}, nil
}
