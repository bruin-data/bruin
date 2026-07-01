package vitess

import (
	"fmt"
	"net/url"
	"strconv"
)

// defaultPort is vtgate's default MySQL-protocol port.
const defaultPort = 15306

// Config describes a connection to a Vitess cluster through vtgate. Vitess speaks the MySQL wire
// protocol, so the actual database connection reuses the MySQL driver; only the ingestr URI scheme
// differs (ingestr now routes Vitess purely by the "vitess" scheme).
type Config struct {
	Username    string
	Password    string
	Host        string
	Port        int
	Database    string
	GrpcPort    int
	GrpcHost    string
	GrpcTLS     bool
	SslCaPath   string
	SslCertPath string
	SslKeyPath  string
}

// GetIngestrURI builds the ingestr source/destination URI. It emits the dedicated "vitess" scheme
// (not "mysql"); ingestr rejects "mysql://" pointed at a Vitess server. The vtgate gRPC settings and
// SSL paths are carried as query params so they survive into ingestr's CDC scheme (vitess+cdc://),
// which the ingestr operator derives by appending "+cdc" to this scheme.
func (c Config) GetIngestrURI() string {
	if c.Port == 0 {
		c.Port = defaultPort
	}

	u := &url.URL{
		Scheme: "vitess",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   c.Database,
	}

	q := u.Query()
	if c.GrpcPort != 0 {
		q.Set("grpc_port", strconv.Itoa(c.GrpcPort))
	}
	if c.GrpcHost != "" {
		q.Set("grpc_host", c.GrpcHost)
	}
	if c.GrpcTLS {
		q.Set("grpc_tls", "true")
	}
	if c.SslCaPath != "" {
		q.Set("ssl_ca", c.SslCaPath)
	}
	if c.SslCertPath != "" {
		q.Set("ssl_cert", c.SslCertPath)
	}
	if c.SslKeyPath != "" {
		q.Set("ssl_key", c.SslKeyPath)
	}
	if encoded := q.Encode(); encoded != "" {
		u.RawQuery = encoded
	}

	return u.String()
}

// ToDBConnectionURI builds the go-sql-driver/mysql DSN used to open a direct connection (e.g. for
// `bruin connections test`). Vitess is reached over vtgate's MySQL-protocol port.
func (c Config) ToDBConnectionURI() string {
	if c.Port == 0 {
		c.Port = defaultPort
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?multiStatements=true",
		c.Username,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
	)

	// Only the MySQL-protocol SSL settings govern TLS on this DSN. GrpcTLS is intentionally
	// excluded: it configures the independent vtgate gRPC (VStream) transport, which has a
	// separate port and TLS setup, so honoring it here would wrongly force TLS on the
	// MySQL-protocol connection used by `bruin connections test`.
	if c.SslCaPath != "" || c.SslCertPath != "" || c.SslKeyPath != "" {
		dsn += "&tls=true"
	}

	return dsn
}

// interface guard: the shared mysql.Client accepts any config exposing these two methods.
var _ interface {
	GetIngestrURI() string
	ToDBConnectionURI() string
} = Config{}
