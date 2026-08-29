package ripestat

type Config struct{}

func (c *Config) GetIngestrURI() string {
	return "ripestat://"
}
