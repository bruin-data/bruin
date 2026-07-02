package planetscale

import (
	"fmt"
	"net/url"
)

// defaultPort is PlanetScale's MySQL-protocol port.
const defaultPort = 3306

// Config describes a connection to PlanetScale, the managed Vitess platform. PlanetScale speaks the
// MySQL wire protocol, so the direct database connection reuses the MySQL driver; the ingestr URI
// uses the dedicated "ps_mysql" scheme, which ingestr routes through its hosted psdbconnect API
// (TLS is enabled automatically, so no SSL configuration is required). ingestr renamed this scheme
// from "planetscale" to "ps_mysql" in v1.0.62 to make room for PlanetScale Postgres.
type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
}

// GetIngestrURI builds the ingestr source/destination URI using the dedicated "ps_mysql" scheme.
// ingestr enables TLS automatically for this scheme, and the ingestr operator derives the CDC scheme
// (ps_mysql+cdc://) by appending "+cdc". Note the scheme contains an underscore: url.URL.String()
// emits it fine, but net/url.Parse rejects it, so the ingestr operator extracts the scheme manually
// rather than round-tripping this URI through url.Parse.
func (c Config) GetIngestrURI() string {
	if c.Port == 0 {
		c.Port = defaultPort
	}

	u := &url.URL{
		Scheme: "ps_mysql",
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
