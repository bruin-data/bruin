package flightsql

import (
	"net"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Config holds the connection parameters for an Arrow Flight SQL platform.
//
// Flight SQL is a wire protocol rather than a SQL dialect, so the same
// connection settings work against any Flight SQL compatible engine (Dremio
// being the first one we support). The Dialect field selects the SQL dialect
// used when rendering materializations (see dialect.go); it defaults to
// "dremio" when left empty.
type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	Dialect  string
}

// ToDSN builds a DSN understood by the ADBC Flight SQL database/sql driver.
// The driver parses the DSN as a ";"-separated list of "key=value" options,
// where the keys are the standard ADBC option names (uri, username, password).
//
// The driver does not unescape values, so credentials are passed through
// verbatim; a ";" in a credential would split the DSN into a bogus option and
// fail to parse. We reject that explicitly rather than corrupt the value. ("="
// is safe because the driver splits each option on its first "=" only.)
func (c Config) ToDSN() (string, error) {
	if strings.Contains(c.Username, ";") || strings.Contains(c.Password, ";") {
		return "", errors.New("Flight SQL username and password cannot contain ';' (the DSN option delimiter)")
	}

	uri := "grpc+tcp://" + net.JoinHostPort(c.Host, strconv.Itoa(c.Port))

	opts := []string{"uri=" + uri}
	if c.Username != "" {
		opts = append(opts, "username="+c.Username)
	}
	if c.Password != "" {
		opts = append(opts, "password="+c.Password)
	}

	return strings.Join(opts, ";"), nil
}

func (c Config) GetDatabase() string {
	return c.Database
}
