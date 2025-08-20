package fluxx

type Client struct {
	config FluxxConfig
}

type FluxxConfig interface {
	GetIngestrURI() string
}

func NewClient(c FluxxConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
