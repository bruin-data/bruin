package googleanalytics

type Client struct {
	config Config
}

func NewClient(c Config) (*Client, error) {
	return &Client{c}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
