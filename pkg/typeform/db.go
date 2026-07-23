package typeform

type Client struct {
	config Config
}

type TypeformConfig interface {
	GetIngestrURI() string
}

func NewClient(c Config) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
