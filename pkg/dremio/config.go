package dremio

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

// Config holds the connection parameters for a Dremio platform.
//
// Dremio is reached over the Arrow Flight SQL wire protocol, which in Go is
// served by the apache/arrow-adbc Flight SQL driver. Authentication is either
// username/password (Dremio Software) or a bearer Token / Personal Access Token
// (Dremio Cloud); the two are mutually exclusive. TLS must be enabled for
// Dremio Cloud.
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
		return "", errors.New("Dremio credentials cannot contain ';' (the DSN option delimiter)")
	}
	if c.Token != "" && (c.Username != "" || c.Password != "") {
		// The driver itself rejects this combination; fail early with a clearer message.
		return "", errors.New("Dremio connection cannot set both token and username/password")
	}

	scheme := "grpc+tcp"
	if c.TLS {
		scheme = "grpc+tls"
	}
	uri := scheme + "://" + net.JoinHostPort(c.Host, strconv.Itoa(c.Port))

	opts := []string{"uri=" + uri}

	if c.Token != "" {
		// Dremio Cloud (and other token-based engines) authenticate via a
		// bearer Authorization header rather than username/password.
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
