package planetscale

import (
	"fmt"
	"net/url"
)

// defaultPort is PlanetScale's MySQL-protocol port.
const defaultPort = 3306

// Config describes a connection to PlanetScale, the managed Vitess platform. PlanetScale speaks the
// MySQL wire protocol, so the direct database connection reuses the MySQL driver; the ingestr URI
// uses the dedicated "planetscale" scheme, which ingestr routes through its hosted psdbconnect API
// (TLS is enabled automatically, so no SSL configuration is required).
type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
}

// GetIngestrURI builds the ingestr source/destination URI using the dedicated "planetscale" scheme.
// ingestr enables TLS automatically for this scheme, and the ingestr operator derives the CDC scheme
// (planetscale+cdc://) by appending "+cdc".
func (c Config) GetIngestrURI() string {
	if c.Port == 0 {
		c.Port = defaultPort
	}

	u := &url.URL{
		Scheme: "planetscale",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.Database,
	}

	return u.String()
}

// ToDBConnectionURI builds the go-sql-driver/mysql DSN used to open a direct connection (e.g. for
// `bruin connections test`). PlanetScale requires encrypted connections, so TLS is always enabled;
// go-sql-driver's tls=true verifies the server certificate against the system root CAs, which is
// valid for the public *.psdb.cloud endpoints.
func (c Config) ToDBConnectionURI() string {
	if c.Port == 0 {
		c.Port = defaultPort
	}

	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?tls=true&multiStatements=true",
		c.Username,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
	)
}

// interface guard: the shared mysql.Client accepts any config exposing these two methods.
var _ interface {
	GetIngestrURI() string
	ToDBConnectionURI() string
} = Config{}
