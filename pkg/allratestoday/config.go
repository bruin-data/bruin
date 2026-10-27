package allratestoday

type Config struct{}

func (c *Config) GetIngestrURI() string {
	return "allratestoday://"
}
