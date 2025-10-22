package allium

type Client struct {
	config AlliumConfig
}

type AlliumConfig interface {
	GetIngestrURI() string
}

func NewClient(c AlliumConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
