package stripe

type Client struct {
	config StripeConfig
}

type StripeConfig interface {
	GetIngestrURI() string
}

func NewClient(c StripeConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
