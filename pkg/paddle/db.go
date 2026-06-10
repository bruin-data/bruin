package paddle

type Client struct {
	config PaddleConfig
}

type PaddleConfig interface {
	GetIngestrURI() string
}

func NewClient(c PaddleConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
