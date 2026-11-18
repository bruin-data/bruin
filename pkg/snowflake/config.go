package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/youmark/pkcs8"
)

const snowflakeDomain = ".snowflakecomputing.com"

type Config struct {
	ReadOnly   bool
	Account    string
	Host       string
	Port       int
	Username   string
	Password   string
	Region     string
	Role       string
	Database   string
	Schema     string
	Warehouse  string
	PrivateKey string
	Token      string
}

func (c Config) DSN() (string, error) {
	authType := gosnowflake.AuthTypeSnowflake
	if c.PrivateKey != "" {
		authType = gosnowflake.AuthTypeJwt
	} else if c.Token != "" {
		authType = gosnowflake.AuthTypeOAuth
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
		Host:          hostWithRegion(c.Host, c.Account, c.Region),
		Port:          c.Port,
		User:          c.Username,
		Password:      c.Password,
		Region:        c.Region,
		Role:          c.Role,
		Database:      c.Database,
		Schema:        c.Schema,
		Warehouse:     c.Warehouse,
		PrivateKey:    privateKey,
		Token:         c.Token,
		// The LocalStack emulator has no OCSP cache server and its certificate names no OCSP
		// responder, so OCSP would only retry ocsp.<host>:80 for seconds before failing open.
		// gosnowflake can only turn off the cache server process-wide, so skip OCSP for these hosts.
		DisableOCSPChecks: isLocalStackHost(c.Host),
	}

	return gosnowflake.DSN(&snowflakeConfig)
}

// gosnowflake v1 inserted the region into a regionless explicit host and v2 does not, so keep
// that for the one shape where it is correct: a legacy locator account's <account>.snowflakecomputing.com.
// The region may also come from the account, as in xy12345.eu-west-1.
func hostWithRegion(host, account, region string) string {
	region = strings.TrimSpace(region)
	if region == "" {
		account, region, _ = strings.Cut(account, ".")
	}
	if region == "" || !strings.EqualFold(host, account+snowflakeDomain) || !isAccountLocator(account) {
		return host
	}
	return host[:len(account)] + "." + region + host[len(account):]
}

func isAccountLocator(account string) bool {
	if account == "" {
		return false
	}
	for _, r := range account {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func isLocalStackHost(host string) bool {
	host = strings.ToLower(host)
	return host == "localhost.localstack.cloud" || strings.HasSuffix(host, ".localhost.localstack.cloud")
}

func (c Config) GetIngestrURI() (string, error) {
	if c.ReadOnly {
		return "", errors.New("read-only Snowflake connections cannot be used with ingestr because SQL validation cannot be enforced")
	}

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
	if c.Host != "" {
		values.Add("host", c.Host)
	}
	if c.Port != 0 {
		values.Add("port", strconv.Itoa(c.Port))
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
	if c.Account == "" {
		return false
	}
	if c.Token != "" {
		return true
	}
	if c.PrivateKey != "" {
		return c.Username != ""
	}
	return c.Username != "" && c.Password != "" && c.Region != ""
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
