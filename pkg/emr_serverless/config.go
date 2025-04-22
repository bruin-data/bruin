package emr_serverless

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
)

type MissingFieldsError struct {
	Fields []string
}

func (e *MissingFieldsError) Error() string {
	return fmt.Sprintf("missing required fields: %v", strings.Join(e.Fields, ", "))
}

type Config struct {
	AccessKey     string `yaml:"access_key"`
	SecretKey     string `yaml:"secret_key"`
	ApplicationID string `yaml:"application_id"`
	ExecutionRole string `yaml:"execution_role"`
	Region        string `yaml:"region"`
	Workspace     string `yaml:"workspace"`
}

var optionalFields = []string{
	"workspace",
}

func (c *Config) validate() error {
	missing := []string{}
	typ := reflect.TypeOf(c).Elem()
	val := reflect.ValueOf(c).Elem()
	for field := range typ.NumField() {
		fieldName := typ.Field(field).Tag.Get("yaml")
		if slices.Contains(optionalFields, fieldName) {
			continue
		}

		fieldValue := val.Field(field).String()
		if fieldValue == "" {
			missing = append(missing, fieldName)
		}
	}
	if len(missing) > 0 {
		return &MissingFieldsError{Fields: missing}
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
