package gorgias

type Client struct {
	config GorgiasConfig
}

type GorgiasConfig interface {
	GetIngestrURI() string
}

func NewClient(c GorgiasConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
