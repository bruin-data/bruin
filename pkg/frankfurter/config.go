package frankfurter

type Config struct {
}

func (c *Config) GetIngestrURI() string {
	return "frankfurter://"
}
