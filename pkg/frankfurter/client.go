package frankfurter

type Client struct {
	config Config
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

func NewClient(c Config) (*Client, error) {
	return &Client{c}, nil
}
