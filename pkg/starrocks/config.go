package starrocks

import (
	"fmt"
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

// ToDBConnectionURI builds a go-sql-driver/mysql DSN for StarRocks, which speaks
// the MySQL wire protocol on its FE query port (9030 by default). The SSL field
// carries a driver-native tls mode ("true"/"skip-verify"/"preferred"), so it is
// forwarded straight to the driver's built-in tls parameter without registering a
// custom TLS config.
func (c Config) ToDBConnectionURI() (string, error) {
	port := c.Port
	if port == 0 {
		port = defaultPort
	}

	query := url.Values{}
	query.Set("multiStatements", "true")
	query.Set("parseTime", "true")
	if c.SSL != "" {
		query.Set("tls", c.SSL)
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?%s",
		c.Username,
		c.Password,
		c.Host,
		port,
		c.Database,
		query.Encode(),
	)

	return dsn, nil
}
