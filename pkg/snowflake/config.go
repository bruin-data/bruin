package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"

	"github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
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
		privateKey, err = parsePrivateKey(c.PrivateKey, c.Password)
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
		Path:   c.Database,
		Host:   c.Account,
	}
	if c.PrivateKey == "" {
		u.User = url.UserPassword(c.Username, c.Password)
	} else {
		u.User = url.User(c.Username)
	}

	values := u.Query()

	if c.Warehouse != "" {
		values.Add("warehouse", c.Warehouse)
	}
	if c.Role != "" {
		values.Add("role", c.Role)
	}

	if c.PrivateKey != "" {
		values.Add("private_key", c.PrivateKey)
		if c.Password != "" {
			values.Add("private_key_passphrase", c.Password)
		}
	}
	u.RawQuery = values.Encode()
	return u.String(), nil
}

func (c Config) IsValid() bool {
	return c.Account != "" && c.Username != "" && c.Password != "" && c.Region != ""
}

func parsePrivateKey(content string, passphrase string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(content))
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the private key")
	}

	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err == nil {
		pk, ok := privateKey.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("private key is not of type *rsa.PrivateKey")
		}
		return pk, nil
	}
	if passphrase != "" {
		decryptedKey, err := pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt private key with passphrase: %w", err)
		}
		pk, ok := decryptedKey.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("private key is not of type *rsa.PrivateKey")
		}
		return pk, nil
	}
	return nil, fmt.Errorf("failed to parse private key: %w", err)
}
