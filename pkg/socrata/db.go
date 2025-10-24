package socrata

type Client struct {
	config SocrataConfig
}

type SocrataConfig interface {
	GetIngestrURI() string
}

func NewClient(c SocrataConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
