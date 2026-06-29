package recurly

type Client struct {
	config RecurlyConfig
}

type RecurlyConfig interface {
	GetIngestrURI() string
}

func NewClient(c RecurlyConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
