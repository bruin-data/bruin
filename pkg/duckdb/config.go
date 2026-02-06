package duck

import "github.com/bruin-data/bruin/pkg/config"

type Config struct {
	Path      string
	Lakehouse *config.LakehouseConfig
}

// ToDBConnectionURI returns a connection URI to be used with the pgx package.
func (c Config) ToDBConnectionURI() string {
	return c.Path
}

func (c Config) GetIngestrURI() string {
	connString := "duckdb:///" + c.Path

	return connString
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
	Token    string
	Database string
}

func (c MotherDuckConfig) ToDBConnectionURI() string {
	if c.Database != "" {
		return "md:" + c.Database + "?motherduck_token=" + c.Token
	}
	return "md:?motherduck_token=" + c.Token
}

func (c MotherDuckConfig) GetIngestrURI() string {
	if c.Database != "" {
		return "motherduck://" + c.Database + "?token=" + c.Token
	}
	return "motherduck://?token=" + c.Token
}
