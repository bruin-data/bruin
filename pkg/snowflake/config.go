package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/snowflakedb/gosnowflake"
	"net/url"
)

type Config struct {
	Account    string
	Username   string
	Password   string
	Region     string
	Role       string
	Database   string
	Schema     string
	Warehouse  string
	PrivateKey string
}

func (c Config) DSN() (string, error) {
	authType := gosnowflake.AuthTypeSnowflake
	if c.PrivateKey != "" {
		authType = gosnowflake.AuthTypeJwt
	}

	snowflakeConfig := gosnowflake.Config{
		Authenticator: authType,
		Account:       c.Account,
		User:          c.Username,
		Password:      c.Password,
		Region:        c.Region,
		Role:          c.Role,
		Database:      c.Database,
		Schema:        c.Schema,
		Warehouse:     c.Warehouse,
		PrivateKey: func() *rsa.PrivateKey {
			if c.PrivateKey == "" {
				return nil
			}
			key, err := parsePrivateKey(c.PrivateKey)
			if err != nil {
				return nil
			}
			return key
		}(),
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

func parsePrivateKey(content string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(content))
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the private key")
	}
	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pk, ok := privateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("interface convertion. expected type *rsa.PrivateKey, but got %T", privateKey)
	}
	return pk, nil
}
