package sqlite

type Config struct {
	Path string
}

func (c *Config) GetIngestrURI() string {
	return "sqlite:///" + c.Path
}
