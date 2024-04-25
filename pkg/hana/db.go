package hana

type Client struct {
	config NotionConfig
}

type NotionConfig interface {
	GetIngestrURI() string
}

func NewClient(c NotionConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
