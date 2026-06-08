package sail

import (
	"net"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// ADBC Flight SQL option keys (see the apache/arrow-adbc flightsql driver).
const (
	optAuthorizationHeader = "adbc.flight.sql.authorization_header"
	optTLSSkipVerify       = "adbc.flight.sql.client_option.tls_skip_verify"
)

// Config holds the connection parameters for a Sail platform.
//
// Sail (LakeSail / PySail) exposes an Arrow Flight SQL server and speaks Spark
// SQL. Flight SQL is served in Go by the apache/arrow-adbc Flight SQL driver.
// Sail does not require authentication by default; the username/password and
// bearer Token fields are provided for deployments that put auth in front of
// it. Token and username/password are mutually exclusive.
type Config struct {
	Host          string
	Port          int
	Username      string
	Password      string
	Token         string
	Database      string
	TLS           bool
	TLSSkipVerify bool
}

// ToDSN builds a DSN understood by the ADBC Flight SQL database/sql driver.
// The driver parses the DSN as a ";"-separated list of "key=value" options,
// where the keys are the standard ADBC option names (uri, username, password,
// adbc.flight.sql.*).
//
// The driver does not unescape values, so credentials are passed through
// verbatim; a ";" in a credential would split the DSN into a bogus option and
// fail to parse. We reject that explicitly rather than corrupt the value. ("="
// is safe because the driver splits each option on its first "=" only.)
func (c Config) ToDSN() (string, error) {
	if strings.ContainsRune(c.Username+c.Password+c.Token, ';') {
		return "", errors.New("Sail credentials cannot contain ';' (the DSN option delimiter)")
	}
	if c.Token != "" && (c.Username != "" || c.Password != "") {
		// The driver itself rejects this combination; fail early with a clearer message.
		return "", errors.New("Sail connection cannot set both token and username/password")
	}

	scheme := "grpc+tcp"
	if c.TLS {
		scheme = "grpc+tls"
	}
	uri := scheme + "://" + net.JoinHostPort(c.Host, strconv.Itoa(c.Port))

	opts := []string{"uri=" + uri}

	if c.Token != "" {
		// Token-based engines authenticate via a bearer Authorization header
		// rather than username/password.
		opts = append(opts, optAuthorizationHeader+"=Bearer "+c.Token)
	} else {
		if c.Username != "" {
			opts = append(opts, "username="+c.Username)
		}
		if c.Password != "" {
			opts = append(opts, "password="+c.Password)
		}
	}

	if c.TLS && c.TLSSkipVerify {
		opts = append(opts, optTLSSkipVerify+"=true")
	}

	return strings.Join(opts, ";"), nil
}

func (c Config) GetDatabase() string {
	return c.Database
}
