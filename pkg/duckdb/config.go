package duck

type Config struct {
	Path string
}

// ToDBConnectionURI returns a connection URI to be used with the pgx package.
func (c Config) ToDBConnectionURI() string {
	return c.Path
}

func (c Config) GetIngestrURI() string {
	connString := "duckdb:///" + c.Path

	return connString
}
