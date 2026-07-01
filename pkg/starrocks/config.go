package starrocks

import (
	"net"
	"net/url"
	"strconv"
)

const defaultPort = 9030

type Config struct {
	Username       string // Required
	Password       string // Optional
	Host           string // Required
	Port           int    // Optional - defaults to 9030 (FE MySQL protocol port)
	Database       string // Optional - default database for unqualified table names
	Catalog        string // Optional - external lakehouse catalog (Iceberg/Hudi/Hive/...)
	SSL            string // Optional - "true" or "skip-verify" to enable TLS
	HTTPPort       int    // Optional - FE HTTP port for Stream Load when used as a destination
	ReplicationNum int    // Optional - replica count for tables created as a destination
}

// GetIngestrURI builds starrocks://user:pass@host:port/[catalog/]database?ssl=...
// A lone database maps to the internal catalog; catalog is prefixed only when a
// database is also set, matching ingestr's [catalog/]database path handling.
func (c Config) GetIngestrURI() string {
	port := c.Port
	if port == 0 {
		port = defaultPort
	}

	u := &url.URL{
		Scheme: "starrocks",
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(port)),
	}

	if c.Username != "" {
		if c.Password != "" {
			u.User = url.UserPassword(c.Username, c.Password)
		} else {
			u.User = url.User(c.Username)
		}
	}

	if c.Database != "" {
		if c.Catalog != "" {
			u.Path = "/" + c.Catalog + "/" + c.Database
		} else {
			u.Path = "/" + c.Database
		}
	}

	query := url.Values{}
	if c.SSL != "" {
		query.Set("ssl", c.SSL)
	}
	// Destination-only params; the source ignores them.
	if c.HTTPPort != 0 {
		query.Set("http_port", strconv.Itoa(c.HTTPPort))
	}
	if c.ReplicationNum != 0 {
		query.Set("replication_num", strconv.Itoa(c.ReplicationNum))
	}
	u.RawQuery = query.Encode()

	return u.String()
}
