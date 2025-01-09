package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/snowflakedb/gosnowflake"
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

	var privateKey *rsa.PrivateKey
	var err error
	if c.PrivateKey != "" {
		privateKey, err = parsePrivateKey(c.PrivateKey)
		if err != nil {
			return "", fmt.Errorf("failed to parse private key: %w", err)
		}
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
		PrivateKey:    privateKey,
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
	// Decode the PEM block
	block, _ := pem.Decode([]byte(content))
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the private key")
	}

	// Check for keywords in the PEM block type to identify encryption
	if block.Type == "ENCRYPTED PRIVATE KEY" {
		return nil, errors.New("encrypted private keys are not supported at the moment, please provide an unencrypted key")
	}

	// Attempt to parse the private key
	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// Check if the error suggests encryption issues
		if strings.Contains(err.Error(), "encrypted") {
			return nil, errors.New("failed to parse encrypted private key. Provide an unencrypted key")
		}
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Assert the type to *rsa.PrivateKey
	pk, ok := privateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("interface conversion: expected type *rsa.PrivateKey, but got %T", privateKey)
	}

	return pk, nil
}
