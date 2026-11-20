package duck

import (
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/user"
)

const (
	dbcDriverName = "duckdb"
	quackAlias    = "quack_db"
	// QuackDriverVersion pins the DuckDB ADBC driver used for Quack connections
	// and DuckDB 2.0 database files. Both need DuckDB 2.0+, while the default
	// driver stays on a release MotherDuck supports, so the two are installed side by side.
	QuackDriverVersion = "2.0.0"
	// maxDefaultDriverStorageVersion is the newest file header version the
	// default driver reads; DuckDB 2.0 files carry 999 there.
	maxDefaultDriverStorageVersion = 68
)

// IsDuckDB2FileURI reports whether an ingestr duckdb:/// URI points at a
// database file only the DuckDB 2.0 driver can read.
func IsDuckDB2FileURI(uri string) bool {
	path, ok := strings.CutPrefix(uri, "duckdb:///")
	return ok && isDuckDB2File(path)
}

// isDuckDB2File reads the DuckDB file header, a checksum followed by "DUCK" and
// a little-endian version. Missing files are created by the default driver.
func isDuckDB2File(path string) bool {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "md:") {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var header [20]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return false
	}
	return string(header[8:12]) == "DUCK" && binary.LittleEndian.Uint64(header[12:]) > maxDefaultDriverStorageVersion
}

// QuackADBCDriverDir returns the directory holding the Quack DuckDB driver.
func QuackADBCDriverDir() (string, error) {
	return user.VersionedADBCDriverDir("quack", dbcDriverName, QuackDriverVersion)
}

type Config struct {
	Path       string
	ReadOnly   bool
	Lakehouse  *config.LakehouseConfig
	Quack      bool
	Host       string
	Port       int
	Token      string
	DisableSSL bool

	MemoryLimit            string
	Threads                int
	TempDirectory          string
	MaxTempDirectorySize   string
	PreserveInsertionOrder *bool
}

// resourceOptions returns the DuckDB configuration options for the configured
// resource limits, so they apply from the moment the database opens.
func (c Config) resourceOptions() map[string]string {
	opts := map[string]string{}
	if c.MemoryLimit != "" {
		opts["memory_limit"] = c.MemoryLimit
	}
	if c.Threads != 0 {
		opts["threads"] = strconv.Itoa(c.Threads)
	}
	if c.TempDirectory != "" {
		opts["temp_directory"] = c.TempDirectory
	}
	if c.MaxTempDirectorySize != "" {
		opts["max_temp_directory_size"] = c.MaxTempDirectorySize
	}
	if c.PreserveInsertionOrder != nil {
		opts["preserve_insertion_order"] = strconv.FormatBool(*c.PreserveInsertionOrder)
	}
	return opts
}

// ToDBConnectionURI returns a connection URI to be used with the pgx package.
func (c Config) ToDBConnectionURI() string {
	return c.Path
}

func (c Config) GetIngestrURI() string {
	if c.HasQuack() {
		return c.quackIngestrURI()
	}
	if c.HasLakehouse() {
		if uri := BuildIngestrLakehouseURI(c.Lakehouse); uri != "" {
			return uri
		}
	}
	return "duckdb:///" + c.Path
}

func (c Config) HasQuack() bool {
	return c.Quack
}

// usesDuckDB2Driver reports whether the connection needs the DuckDB 2.0 driver
// instead of the default one.
func (c Config) usesDuckDB2Driver() bool {
	return c.HasQuack() || isDuckDB2File(c.Path)
}

// quackIngestrURI builds duckdb://:<token>@host[:port]?quack=true, keeping the
// token in the password slot where URI maskers redact it.
func (c Config) quackIngestrURI() string {
	q := url.Values{"quack": {"true"}}
	if c.DisableSSL {
		q.Set("disable_ssl", "true")
	}
	u := url.URL{Scheme: "duckdb", User: url.UserPassword("", c.Token), Host: c.quackHost(), RawQuery: q.Encode()}
	return u.String()
}

func (c Config) quackHost() string {
	if c.Port != 0 {
		return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	}
	if strings.Contains(c.Host, ":") {
		return "[" + c.Host + "]"
	}
	return c.Host
}

// quackAttachStatements attach the Quack server to a local in-memory DuckDB
// and make it the default catalog, so SQL runs against the remote database.
func (c Config) quackAttachStatements() []string {
	endpoint := quoteSQLStringLiteral("quack:" + c.quackHost())
	attach := "ATTACH " + endpoint + " AS " + quackAlias
	if c.DisableSSL {
		attach += " (DISABLE_SSL true)"
	}
	return []string{
		"INSTALL quack",
		"LOAD quack",
		"CREATE OR REPLACE SECRET bruin_quack (TYPE quack, TOKEN " + quoteSQLStringLiteral(c.Token) + ", SCOPE " + endpoint + ")",
		attach,
		"USE " + quackAlias,
	}
}

func (c Config) HasLakehouse() bool {
	return c.Lakehouse != nil
}

func (c Config) GetLakehouseAlias() string {
	if c.Lakehouse == nil {
		return ""
	}
	// Default alias based on format
	return string(c.Lakehouse.Format) + "_catalog"
}

type MotherDuckConfig struct {
	ReadOnly bool
	Token    string
	Database string
}

func (c MotherDuckConfig) ToDBConnectionURI() string {
	if c.Database != "" {
		return "md:" + c.Database + "?motherduck_token=" + url.QueryEscape(c.Token)
	}
	return "md:?motherduck_token=" + url.QueryEscape(c.Token)
}

func (c MotherDuckConfig) GetIngestrURI() string {
	if c.Database != "" {
		return "motherduck://" + c.Database + "?token=" + url.QueryEscape(c.Token)
	}
	return "motherduck://?token=" + url.QueryEscape(c.Token)
}

func isReadOnlyMotherDuck(c DuckDBConfig) bool {
	switch cfg := c.(type) {
	case MotherDuckConfig:
		return cfg.ReadOnly
	case *MotherDuckConfig:
		return cfg.ReadOnly
	default:
		return false
	}
}
