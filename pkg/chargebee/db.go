package chargebee

type Client struct {
	config ChargebeeConfig
}

type ChargebeeConfig interface {
	GetIngestrURI() string
}

func NewClient(c ChargebeeConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
