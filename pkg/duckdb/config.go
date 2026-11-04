package duck

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
)

const quackAlias = "quack_db"

type Config struct {
	Path       string
	ReadOnly   bool
	Lakehouse  *config.LakehouseConfig
	Quack      bool
	Host       string
	Port       int
	Token      string
	DisableSSL bool
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

// CI experiment marker 1
