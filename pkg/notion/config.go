package notion

type Config struct {
	ApiKey string
}

func (c Config) GetIngestrURI() string {
	return "notion://?api_key=" + c.ApiKey
}
