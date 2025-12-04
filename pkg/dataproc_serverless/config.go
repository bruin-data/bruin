package dataprocserverless

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
	Project               string `yaml:"project"`
	Region                string `yaml:"region"`
	ServiceAccountKey     string `yaml:"service_account_key"`
	ServiceAccountKeyPath string `yaml:"service_account_key_path"`
	Workspace             string `yaml:"workspace"`
}

var optionalFields = []string{
	"service_account_key",
	"service_account_key_path",
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

	// either service_account_key or service_account_key_path must be set
	if c.ServiceAccountKey == "" && c.ServiceAccountKeyPath == "" {
		missing = append(missing, "service_account_key or service_account_key_path")
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
