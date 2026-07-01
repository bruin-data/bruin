package gsc

type Client struct {
	config GSCConfig
}

type GSCConfig interface {
	GetIngestrURI() string
}

func NewClient(c GSCConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
