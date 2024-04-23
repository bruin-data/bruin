package notion

type Config struct {
	Token string
}

func (c Config) GetIngestrURI() string {
	return "notion://?api_key=" + c.Token
}
