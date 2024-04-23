package notion

type Config struct {
	APIKey string
}

func (c Config) GetIngestrURI() string {
	return "notion://?api_key=" + c.APIKey
}
