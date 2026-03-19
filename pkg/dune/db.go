package dune

type Client struct {
	config DuneConfig
}

type DuneConfig interface {
	GetIngestrURI() string
}

func NewClient(c DuneConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
