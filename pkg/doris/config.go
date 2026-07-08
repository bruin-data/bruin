package doris

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
)

const defaultPort = 9030

type Config struct {
	Username    string
	Password    string
	Host        string
	Port        int
	Database    string
	Driver      string
	SslCaPath   string
	SslCertPath string
	SslKeyPath  string
}

func (c Config) GetIngestrURI() string {
	if c.Port == 0 {
		c.Port = defaultPort
	}

	scheme := "mysql"
	if c.Driver != "" {
		scheme += "+" + c.Driver
	} else {
		scheme += "+pymysql"
	}

	u := &url.URL{
		Scheme: scheme,
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.Database,
	}

	if c.SslCaPath != "" || c.SslCertPath != "" || c.SslKeyPath != "" {
		q := u.Query()
		if c.SslCaPath != "" {
			q.Set("ssl_ca", c.SslCaPath)
		}
		if c.SslCertPath != "" {
			q.Set("ssl_cert", c.SslCertPath)
		}
		if c.SslKeyPath != "" {
			q.Set("ssl_key", c.SslKeyPath)
		}
		u.RawQuery = q.Encode()
	}

	return u.String()
}

func (c Config) ToDBConnectionURI() (string, error) {
	if c.Port == 0 {
		c.Port = defaultPort
	}

	query := url.Values{}
	query.Set("multiStatements", "true")
	query.Set("parseTime", "true")

	if c.SslCaPath != "" || c.SslCertPath != "" || c.SslKeyPath != "" {
		tlsConfigName, err := c.registerTLSConfig()
		if err != nil {
			return "", err
		}
		query.Set("tls", tlsConfigName)
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?%s",
		c.Username,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
		query.Encode(),
	)

	return dsn, nil
}

func (c Config) registerTLSConfig() (string, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	if c.SslCaPath != "" {
		caCert, err := os.ReadFile(c.SslCaPath)
		if err != nil {
			return "", fmt.Errorf("failed to read doris ssl_ca_path: %w", err)
		}

		rootCAs := x509.NewCertPool()
		if ok := rootCAs.AppendCertsFromPEM(caCert); !ok {
			return "", fmt.Errorf("failed to parse doris ssl_ca_path %s", c.SslCaPath)
		}
		tlsConfig.RootCAs = rootCAs
	}

	if c.SslCertPath != "" || c.SslKeyPath != "" {
		if c.SslCertPath == "" || c.SslKeyPath == "" {
			return "", errors.New("doris ssl_cert_path and ssl_key_path must be configured together")
		}

		cert, err := tls.LoadX509KeyPair(c.SslCertPath, c.SslKeyPath)
		if err != nil {
			return "", fmt.Errorf("failed to load doris client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	name := c.tlsConfigName()
	if err := mysql.RegisterTLSConfig(name, tlsConfig); err != nil {
		return "", fmt.Errorf("failed to register doris TLS config: %w", err)
	}

	return name, nil
}

func (c Config) tlsConfigName() string {
	sum := sha256.Sum256([]byte(strings.Join([]string{c.Host, c.Database, c.SslCaPath, c.SslCertPath, c.SslKeyPath}, "\x00")))
	return "bruin_doris_" + hex.EncodeToString(sum[:8])
}
