package duck

import (
	"net/url"

	"github.com/bruin-data/bruin/pkg/config"
)

type Config struct {
	Path      string
	ReadOnly  bool
	Lakehouse *config.LakehouseConfig
}

// ToDBConnectionURI returns a connection URI to be used with the pgx package.
func (c Config) ToDBConnectionURI() string {
	return c.Path
}

func (c Config) GetIngestrURI() string {
	if c.HasLakehouse() {
		if uri := BuildIngestrLakehouseURI(c.Lakehouse); uri != "" {
			return uri
		}
	}
	return "duckdb:///" + c.Path
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
